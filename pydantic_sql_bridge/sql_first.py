from collections import defaultdict
import re

import sqlglot.dialects
from sqlglot import parse_one, expressions as exp

from pydantic_sql_bridge.read_write import raw_query
from pydantic_sql_bridge.utils import get_model_name, Cursor, get_database_type, DatabaseType

SqlglotType = exp.DataType.Type
SQLGLOT_TYPE_TO_PYDANTIC = {
    SqlglotType.BIGINT: "int",
    SqlglotType.BIT: "bool",
    SqlglotType.CHAR: "str",
    SqlglotType.DATE: "datetime.date",
    SqlglotType.DATETIME: "datetime.datetime",
    SqlglotType.DECIMAL: "float",
    SqlglotType.FLOAT: "float",
    SqlglotType.INT: "int",
    SqlglotType.NCHAR: "str",
    SqlglotType.NVARCHAR: "str",
    SqlglotType.SMALLINT: "int",
    SqlglotType.VARCHAR: "str",
    SqlglotType.TEXT: "str",
    SqlglotType.UNIQUEIDENTIFIER: "str",
}

COLUMN_DEFINITION_TRANSFORMERS = {
    "optional": lambda typ: f"typing.Optional[{typ}]",
    "primary_key": lambda typ: f"typing.Annotated[{typ}, Annotations.PRIMARY_KEY]",
}


def transform_column_def(
        col_def: exp.ColumnDef, primary_key: set[str]
) -> tuple[str, str]:
    primary_key = set() if primary_key is None else primary_key
    name = col_def.this.this
    pydantic_type = SQLGLOT_TYPE_TO_PYDANTIC[col_def.args["kind"].this]

    to_apply = set()
    for constraint in col_def.args.get("constraints", []):
        if isinstance(
                constraint.kind, exp.NotNullColumnConstraint
        ) and constraint.kind.args.get("allow_null", False):
            to_apply.add("optional")
        if isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint):
            to_apply.add("primary_key")

    if name in primary_key:
        to_apply.add("primary_key")

    for key, transform in COLUMN_DEFINITION_TRANSFORMERS.items():
        if key in to_apply:
            pydantic_type = transform(pydantic_type)
    return name, pydantic_type


def parse_create_table(sql_expr: exp.Create) -> tuple[str, list[tuple[str, str]]]:
    if not isinstance(sql_expr, exp.Create):
        raise ValueError(
            f"Statement not recognized as create table statement {sql_expr=}"
        )

    pk_columns = [
        set(entry.this for entry in expr.expressions)
        for expr in sql_expr.this.expressions
        if isinstance(expr, exp.PrimaryKey)
    ]

    named_constraints = [expr for expr in sql_expr.this.expressions if isinstance(expr, exp.Constraint)]
    pk_constraints = [expr for expr in named_constraints if
                      any(isinstance(sub_expr, exp.PrimaryKeyColumnConstraint) for sub_expr in expr.expressions)]
    pk_names = [node.this.this for expr in pk_constraints for node, parent, key in expr.walk() if
                isinstance(node, exp.Column)]
    pk_columns.append(set(pk_names))

    primary_key = set.union(*pk_columns) if pk_columns else set()

    table_name = sql_expr.this.this.this.this
    column_defs = []
    for sql_col_def in sql_expr.this.expressions:
        if not isinstance(sql_col_def, exp.ColumnDef):
            continue
        if any(
                isinstance(constraint.kind, exp.GeneratedAsRowColumnConstraint)
                and constraint.kind.args.get("hidden")
                for constraint in sql_col_def.constraints
        ):
            continue

        col_def = transform_column_def(sql_col_def, primary_key)
        column_defs.append(col_def)
    return table_name, column_defs


def parse_first_table_name(sql_expr: exp.Expression) -> str:
    if isinstance(sql_expr, exp.Column):
        return sql_expr.table
    else:
        results = [
            node
            for node, parent, key in sql_expr.walk()
            if isinstance(node, exp.Column)
        ]
        if not results:
            raise ValueError(
                f"Cannot find column definition to get table from in {sql_expr}"
            )
        return results[0].table


def strip_annotations(typ: str) -> str:
    match = re.match(r"typing.Annotated\[(.*), .*]", typ)
    if match:
        typ = match[1]
    return typ


def get_optional_tables_and_aliases(select: exp.Select) -> set[str]:
    """Find which tables might have NULL rows in them due to join structuring.
    i.e. with LEFT JOIN, the right-hand side of the join might consist of entirely NULL values
    """
    base = select.args["from"]
    joins = select.args["joins"]
    optional, seen = set(), {base}
    for join in joins:
        if join.side == "RIGHT":
            optional |= seen
        elif join.side == "LEFT":
            optional.add(join.this.name)
            if join.this.alias:
                optional.add(join.this.alias)
        elif join.side == "FULL":
            optional = (
                    {base.name}
                    | {b.alias for b in [base] if b.alias}
                    | {join.this.name for join in joins}
                    | {join.this.alias for join in joins if join.this.alias}
            )

        seen.add(join.this.name)
        if join.this.alias:
            seen.add(join.this.alias)
    return optional


def parse_select_col(col: exp.Expression) -> tuple[str, tuple[str, str], bool]:
    if isinstance(col, exp.Alias):
        name = col.alias
        _, type_source, not_nullable = parse_select_col(col.this)
    elif isinstance(col, exp.Coalesce):
        name, type_source, not_nullable = parse_select_col(col.this)
        fallbacks = [parse_select_col(e) for e in col.expressions]
        not_nullable = any(
            not_nullable for name, type_source, not_nullable in fallbacks
        )
    elif isinstance(col, exp.Column):
        name = col.name
        type_source = col.table, col.name
        not_nullable = False
    elif isinstance(col, exp.Null):
        name, type_source, not_nullable = "", ("", ""), False
    elif isinstance(col, exp.Literal):
        name, type_source, not_nullable = "", ("", ""), True
    else:
        raise ValueError(f"Unsupported column type {type(col)}: {col}")
    return name, type_source, not_nullable


def parse_create_view(
        sql_expr: exp.Create, models: dict[str, dict[str, str]]
) -> tuple[str, list[tuple[str, str]]]:
    join_source = sql_expr.expression.args["from"]
    alias_to_model = {join_source.alias_or_name: models.get(join_source.this.name)} | {
        join.this.alias_or_name: models.get(join.this.name)
        for join in sql_expr.expression.args["joins"]
    }

    columns = sql_expr.expression.expressions
    tables = [parse_first_table_name(col) for col in columns]
    if any(
            missing_aliases := {table for table in tables if table not in alias_to_model}
    ):
        raise ValueError(f"Cannot translate aliases {missing_aliases} to models.")

    optional = get_optional_tables_and_aliases(sql_expr.expression)

    column_defs = []
    for col, table in zip(columns, tables):
        col_name, (source_table, source_col), has_non_null_fallback = parse_select_col(col)
        col_model = strip_annotations(alias_to_model[source_table][source_col])
        if table in optional and not re.match(r"typing.Optional", col_model) and not has_non_null_fallback:
            col_model = f"typing.Optional[{col_model}]"
        column_defs.append((col.alias_or_name, col_model))

    view_name = sql_expr.this.this.this
    return view_name, column_defs


def to_pydantic_model(sql_expr: exp.Create) -> str:
    if sql_expr.args["kind"] == "VIEW":
        table_name, column_defs = parse_create_view(sql_expr)
    else:
        table_name, column_defs = parse_create_table(sql_expr)
    head = [
        f"class {get_model_name(table_name)}(BaseModel):",
        f'    query_name: typing.ClassVar[str] = "{table_name}"',
    ]
    result = "\n".join(head + [f"    {name}: {typ}" for name, typ in column_defs])
    return result


def create_models_from_sql(
        sql: list[str], dialect: sqlglot.Dialects = sqlglot.dialects.SQLite
) -> str:
    sql_exprs = [parse_one(sql_stmt, dialect=dialect) for sql_stmt in sql]
    if any(
            non_create_exprs := [
                sql_expr for sql_expr in sql_exprs if not isinstance(sql_expr, exp.Create)
            ]
    ):
        raise ValueError(
            f"Cannot parse {non_create_exprs} because they do not appear to be valid create expressions"
        )
    view_exprs = [
        sql_expr
        for sql_expr in sql_exprs
        if isinstance(sql_expr, exp.Create) and sql_expr.args["kind"] == "VIEW"
    ]
    table_exprs = [
        sql_expr
        for sql_expr in sql_exprs
        if isinstance(sql_expr, exp.Create) and sql_expr.args["kind"] != "VIEW"
    ]

    to_join = [
        "from pydantic import BaseModel\n"
        "from pydantic_sql_bridge.utils import Annotations\n"
        "import typing"
    ]

    model_specs = {}
    for table_expr in table_exprs:
        table_name, column_defs = parse_create_table(table_expr)
        # Make sure we can find these models in our views later
        model_specs[table_name] = dict(column_defs)

        model_name = get_model_name(table_name)
        head = [
            f"class {model_name}(BaseModel):",
            f'    query_name: typing.ClassVar[str] = "{table_name}"',
        ]

        model_code = "\n".join(
            head + [f"    {name}: {typ}" for name, typ in column_defs]
        )
        to_join.append(model_code)

        if "datetime" in model_code and "datetime" not in to_join[0]:
            to_join[0] = "import datetime\n" + to_join[0]

    for view_expr in view_exprs:
        view_name, column_defs = parse_create_view(view_expr, model_specs)
        model_name = get_model_name(view_name)

        head = [
            f"class {model_name}(BaseModel):",
            f'    query_name: typing.ClassVar[str] = "{view_name}"',
        ]
        model_code = "\n".join(
            head + [f"    {name}: {typ}" for name, typ in column_defs]
        )
        to_join.append(model_code)

    return "\n\n\n".join(to_join) + "\n"


def create_models_from_db(c: Cursor) -> str:
    """
    How to implement? Couple possible approaches:
    1. Generate SQL based on database, call `create_models_from_sql`
    2. Refactor to generate BaseModels based on internal representation, then load internal representation

    Option 1 is uglier and less efficient at runtime, but relatively easy to program probably.
    Option 2 is neater, but requires us to have an IR which might be tricky to get right.
    """
    db_type = get_database_type(c)
    if db_type == DatabaseType.MSSQL:
        pk_data = raw_query(
            c,
            'select t.name as table_name, c.name as column_name from sys.key_constraints kc '
            'inner join sys.index_columns ic on kc.parent_object_id = ic.object_id '
            'inner join sys.columns c on ic.object_id = c.object_id and ic.index_column_id = c.column_id '
            'inner join sys.tables t on t.object_id = ic.object_id '
            'where kc.type=\'PK\''
        )
        primary_keys = defaultdict(list)
        for record in pk_data:
            primary_keys[record['table_name']].append(record['column_name'])

        table_data = raw_query(
            c,
            'select * from information_schema.columns where table_schema != \'sys\''
        )
        tables = defaultdict(list)
        for record in table_data:
            tables[record['TABLE_NAME']].append(
                (record['COLUMN_NAME'], record['DATA_TYPE'], record['NUMERIC_PRECISION'], record['NUMERIC_SCALE']))

        to_join = []
        for table_name, columns in tables.items():
            to_join.append(f'CREATE TABLE {table_name}(')
            for name, typ, precision, scale in columns:
                typ = f'{typ}({scale}, {precision})' if typ.lower() == 'decimal' else typ
                to_join.append(f'    [{name}] {typ.upper()},')
            if table_name in primary_keys:
                pk = ', '.join(primary_keys[table_name])
                to_join.append(f'    CONSTRAINT PK_{table_name} PRIMARY KEY CLUSTERED ({pk}),')
            to_join[-1] = to_join[-1][:-1]  # strip trailing comma
            to_join.append(')\n')  # closing parenthesis and blank line
        sql = '\n'.join(to_join).split('\n\n')
        return create_models_from_sql(sql, sqlglot.dialects.Dialects.TSQL)
    elif db_type == DatabaseType.SQLITE:
        raise NotImplementedError
    else:
        raise NotImplementedError
