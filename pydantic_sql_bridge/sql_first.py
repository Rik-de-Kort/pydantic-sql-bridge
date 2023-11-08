from collections import deque

import sqlglot.dialects
from sqlglot import parse_one, expressions as exp

from pydantic_sql_bridge.utils import get_model_name

SqlglotType = exp.DataType.Type
SQLGLOT_TYPE_TO_PYDANTIC = {
    SqlglotType.BIGINT: 'int',
    SqlglotType.BIT: 'bool',
    SqlglotType.CHAR: 'str',
    SqlglotType.DATE: 'datetime.date',
    SqlglotType.DATETIME: 'datetime.datetime',
    SqlglotType.DECIMAL: 'float',
    SqlglotType.FLOAT: 'float',
    SqlglotType.INT: 'int',
    SqlglotType.NCHAR: 'str',
    SqlglotType.NVARCHAR: 'str',
    SqlglotType.SMALLINT: 'int',
    SqlglotType.VARCHAR: 'str',
    SqlglotType.TEXT: 'str',
}


def transform_column_def(col_def: exp.ColumnDef) -> tuple[str, str]:
    name = col_def.this.this
    pydantic_type = SQLGLOT_TYPE_TO_PYDANTIC[col_def.args['kind'].this]

    to_apply = deque()
    for constraint in col_def.args.get('constraints', []):
        if isinstance(constraint.kind, exp.NotNullColumnConstraint) and constraint.kind.args.get('allow_null', False):
            to_apply.appendleft(lambda typ: f'typing.Optional[{typ}]')
        if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
            to_apply.append(lambda typ: f'typing.Annotated[{pydantic_type}, Annotations.PRIMARY_KEY]')

    for transform in to_apply:
        pydantic_type = transform(pydantic_type)
    return name, pydantic_type


def parse_create_table(sql_expr: exp.Create) -> tuple[str, list[tuple[str, str]]]:
    if not isinstance(sql_expr, exp.Create):
        raise ValueError(f'Statement not recognized as create table statement {sql_expr=}')

    table_name = sql_expr.this.this.this.this
    column_defs = []
    for sql_col_def in sql_expr.this.expressions:
        if not isinstance(sql_col_def, exp.ColumnDef):
            continue
        if any(
                isinstance(constraint.kind, exp.GeneratedAsRowColumnConstraint) and constraint.kind.args.get('hidden')
                for constraint in sql_col_def.constraints
        ):
            continue

        col_def = transform_column_def(sql_col_def)
        column_defs.append(col_def)
    return table_name, column_defs


def parse_first_table_name(sql_expr: exp.Expression) -> str:
    if isinstance(sql_expr, exp.Column):
        return sql_expr.table
    else:
        results = [node for node, parent, key in sql_expr.walk() if isinstance(node, exp.Column)]
        if not results:
            raise ValueError(f'Cannot find column definition to get table from in {sql_expr}')
        return results[0].table


def strip_annotations(typ: str) -> str:
    import re
    match = re.match(r'typing.Annotated\[(.*), .*]', typ)
    if match:
        typ = match[1]
    return typ


def parse_create_view_joined(
        sql_expr: exp.Create,
        models: dict[str, dict[str, str]]
) -> tuple[str, list[tuple[str, str]]]:
    join_source = sql_expr.expression.args['from']

    alias_to_name = {join_source.this.alias_or_name: join_source.this.this.this}
    alias_to_name |= {join.this.alias_or_name: join.this.this.this for join in sql_expr.expression.args['joins']}
    alias_to_model = {alias: models.get(name) for alias, name in alias_to_name.items()}

    columns = sql_expr.expression.expressions
    tables = [parse_first_table_name(col) for col in columns]
    if any(missing_aliases := {table for table in tables if table not in alias_to_model}):
        matching_names = {alias: alias_to_name.get(alias) for alias in missing_aliases}
        raise ValueError(f'Cannot translate aliases {missing_aliases} to models. '
                         f'Got name matches {matching_names} and models {name_to_model}')

    column_defs = []
    for col, table in zip(columns, tables):
        col_name = col.this
        while not isinstance(col_name, str):  # Todo: should we parse this out properly?
            col_name = col_name.this
        col_model = strip_annotations(alias_to_model[table][col_name])
        column_defs.append((col_name, col_model))

    view_name = sql_expr.this.this.this
    return view_name, column_defs


def to_pydantic_model(sql_expr: exp.Create) -> str:
    if sql_expr.args['kind'] == 'VIEW':
        table_name, column_defs = parse_create_view_joined(sql_expr)
    else:
        table_name, column_defs = parse_create_table(sql_expr)
    head = [f'class {get_model_name(table_name)}(BaseModel):',
            f'    query_name: typing.ClassVar[str] = "{table_name}"']
    result = '\n'.join(head + [f'    {name}: {typ}' for name, typ in column_defs])
    return result


def create_models_from_sql(sql: list[str], dialect: sqlglot.Dialects = sqlglot.dialects.SQLite) -> str:
    sql_exprs = [parse_one(sql_stmt, dialect=dialect) for sql_stmt in sql]
    if any(non_create_exprs := [sql_expr for sql_expr in sql_exprs if not isinstance(sql_expr, exp.Create)]):
        raise ValueError(f'Cannot parse {non_create_exprs} because they do not appear to be valid create expressions')
    view_exprs = [sql_expr for sql_expr in sql_exprs if sql_expr.args['kind'] == 'VIEW']
    table_exprs = [sql_expr for sql_expr in sql_exprs if sql_expr.args['kind'] != 'VIEW']

    to_join = [
        'from pydantic import BaseModel\n'
        'from pydantic_sql_bridge.utils import Annotations\n'
        'import typing'
    ]

    model_specs = {}
    for table_expr in table_exprs:
        table_name, column_defs = parse_create_table(table_expr)
        # Make sure we can find these models in our views later
        model_specs[table_name] = dict(column_defs)

        model_name = get_model_name(table_name)
        head = [f'class {model_name}(BaseModel):',
                f'    query_name: typing.ClassVar[str] = "{table_name}"']

        model_code = '\n'.join(head + [f'    {name}: {typ}' for name, typ in column_defs])
        to_join.append(model_code)

        if 'datetime' in model_code and 'datetime' not in to_join[0]:
            to_join[0] = 'import datetime\n' + to_join[0]

    for view_expr in view_exprs:
        view_name, column_defs = parse_create_view_joined(view_expr, model_specs)
        model_name = get_model_name(view_name)

        head = [f'class {model_name}(BaseModel):',
                f'    query_name: typing.ClassVar[str] = "{view_name}"']
        model_code = '\n'.join(head + [f'    {name}: {typ}' for name, typ in column_defs])
        to_join.append(model_code)

    return '\n\n\n'.join(to_join) + '\n'
