from collections import deque
from typing import Type, Annotated, ClassVar, Optional

import sqlglot.dialects
from pydantic import BaseModel
from sqlglot import parse_one, expressions as expr

from pydantic_sql_bridge.utils import get_model_name, get_table_name, Annotations

SqlglotType = expr.DataType.Type
SQLGLOT_TYPE_TO_PYDANTIC = {
    SqlglotType.BIGINT: 'int',
    SqlglotType.BIT: 'bool',
    SqlglotType.CHAR: 'str',
    SqlglotType.INT: 'int',
    SqlglotType.NCHAR: 'str',
    SqlglotType.NVARCHAR: 'str',
    SqlglotType.TEXT: 'str',
    SqlglotType.VARCHAR: 'str',
}


def transform_column_def(col_def: expr.ColumnDef) -> tuple[str, str]:
    name = col_def.this.this
    pydantic_type = SQLGLOT_TYPE_TO_PYDANTIC[col_def.args['kind'].this]

    to_apply = deque()
    for constraint in col_def.args.get('constraints', []):
        if isinstance(constraint.kind, expr.NotNullColumnConstraint) and constraint.kind.args.get('allow_null'):
            to_apply.appendleft(lambda typ: f'Optional[{typ}]')
        if isinstance(constraint.kind, expr.PrimaryKeyColumnConstraint):
            to_apply.append(lambda typ: f'Annotated[{pydantic_type}, Annotations.PRIMARY_KEY]')

    for transform in to_apply:
        pydantic_type = transform(pydantic_type)
    return name, pydantic_type


def parse_create_table(sql_expr: expr.Create) -> tuple[str, list[tuple[str, str]]]:
    if not isinstance(sql_expr, expr.Create):
        raise ValueError(f'Statement not recognized as create table statement {sql_expr=}')

    table_name = sql_expr.this.this.this.this
    column_defs = []
    for sql_col_def in sql_expr.this.expressions:
        if not isinstance(sql_col_def, expr.ColumnDef):
            continue
        if any(
                isinstance(constraint.kind, expr.GeneratedAsRowColumnConstraint) and constraint.kind.args.get('hidden')
                for constraint in sql_col_def.constraints
        ):
            continue

        col_def = transform_column_def(sql_col_def)
        column_defs.append(col_def)
    return table_name, column_defs


# Todo: refactor this to take a dictionary of {table_name: (column_name, column_typestring)} instead of models
def parse_create_view_joined(sql_expr: expr.Create, models: list[Type[BaseModel]]) -> tuple[str, list[tuple[str, str]]]:
    join_source = sql_expr.expression.args['from']

    alias_to_name = {join_source.this.alias: join_source.this.this.this} | {join.this.alias: join.this.this.this for
                                                                            join in sql_expr.expression.args['joins']}
    name_to_model = {get_table_name(model): model for model in models}
    alias_to_model = {alias: name_to_model.get(name) for alias, name in alias_to_name.items()}

    columns = sql_expr.expression.expressions
    if any(missing_aliases := {col.table for col in columns if col.table not in alias_to_model}):
        matching_names = {alias: alias_to_name.get(alias) for alias in missing_aliases}
        raise ValueError(f'Cannot translate aliases {missing_aliases} to models. '
                         f'Got name matches {matching_names} and models {name_to_model}')
    column_defs = []
    for col in columns:
        col_name = col.this.this
        col_model = alias_to_model[col.table].model_fields[col_name].annotation.__name__
        column_defs.append((col_name, col_model))

    view_name = sql_expr.this.this.this
    return view_name, column_defs


def to_pydantic_model(sql_expr: expr.Create) -> str:
    if sql_expr.args['kind'] == 'VIEW':
        table_name, column_defs = parse_create_view_joined(sql_expr)
    else:
        table_name, column_defs = parse_create_table(sql_expr)
    head = [f'class {get_model_name(table_name)}(BaseModel):',
            f'    query_name: ClassVar[str] = "{table_name}"']
    result = '\n'.join(head + [f'    {name}: {typ}' for name, typ in column_defs])
    return result


def create_models_from_sql(sql: list[str], dialect: sqlglot.Dialects = sqlglot.dialects.SQLite) -> str:
    sql_exprs = [parse_one(sql_stmt, dialect=dialect) for sql_stmt in sql]
    if any(non_create_exprs := [sql_expr for sql_expr in sql_exprs if not isinstance(sql_expr, expr.Create)]):
        raise ValueError(f'Cannot parse {non_create_exprs} because they do not appear to be valid create expressions')
    view_exprs = [sql_expr for sql_expr in sql_exprs if sql_expr.args['kind'] == 'VIEW']
    table_exprs = [sql_expr for sql_expr in sql_exprs if sql_expr.args['kind'] != 'VIEW']

    to_join = [
        'from pydantic import BaseModel\n'
        'from pydantic_sql_bridge.utils import Annotations\n'
        'from typing import Annotated, ClassVar, Optional'
    ]

    model_names = []
    for table_expr in table_exprs:
        table_name, column_defs = parse_create_table(table_expr)
        model_name = get_model_name(table_name)

        head = [f'class {model_name}(BaseModel):',
                f'    query_name: ClassVar[str] = "{table_name}"']
        model_code = '\n'.join(head + [f'    {name}: {typ}' for name, typ in column_defs])
        to_join.append(model_code)

        # Make sure we can find these models in our views later
        # Todo: ideally of course you map this properly or something...
        exec(model_code)
        model_names.append(model_name)

    # You cannot put locals() inside the generator expression here,
    # since it will only contain model_name and the iterator.
    current_locals = locals()
    models = [current_locals[model_name] for model_name in model_names]

    for view_expr in view_exprs:
        view_name, column_defs = parse_create_view_joined(view_expr, models)
        model_name = get_model_name(view_name)

        head = [f'class {model_name}(BaseModel):',
                f'    query_name: ClassVar[str] = "{view_name}"']
        model_code = '\n'.join(head + [f'    {name}: {typ}' for name, typ in column_defs])
        to_join.append(model_code)

    return '\n\n'.join(to_join)
