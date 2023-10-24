from sqlglot import parse_one, expressions as expr

from pydantic_sql_bridge.utils import get_model_name

SqlglotType = expr.DataType.Type
SQLGLOT_TYPE_TO_PYDANTIC = {
    SqlglotType.INT: 'int',
    SqlglotType.TEXT: 'str',
    SqlglotType.BIT: 'bool',
    SqlglotType.NCHAR: 'str',
    SqlglotType.NVARCHAR: 'str',
    SqlglotType.BIGINT: 'int',
}


def transform_column_def(col_def: expr.ColumnDef) -> tuple[str, str]:
    name = col_def.this.this
    pydantic_type = SQLGLOT_TYPE_TO_PYDANTIC[col_def.args['kind'].this]
    for constraint in col_def.args.get('constraints', []):
        if isinstance(constraint.kind, expr.PrimaryKeyColumnConstraint):
            pydantic_type = f'Annotated[{pydantic_type}, Annotations.PRIMARY_KEY]'
    return name, pydantic_type


def parse_create_table(sql_stmt) -> tuple[str, list[tuple[str, str]]]:
    sql_expr = parse_one(sql_stmt)
    if not isinstance(sql_expr, expr.Create):
        raise ValueError(f'Statement not recognized as create table statement {sql_stmt=}')

    table_name = sql_expr.this.this.this.this
    column_defs = [transform_column_def(col_def)
                   for col_def in sql_expr.this.expressions
                   if isinstance(col_def, expr.ColumnDef)]
    return table_name, column_defs


def to_pydantic_model(sql_stmt) -> str:
    table_name, column_defs = parse_create_table(sql_stmt)
    head = f'class {get_model_name(table_name)}(BaseModel):\n'
    body = '\n'.join(f'    {name}: {typ}' for name, typ in column_defs)
    result = head + body
    return result


def create_models_from_sql(sql: list[str]) -> str:
    to_join = [
        'from pydantic import BaseModel\n'
        'from pydantic_sql_bridge.sql_first import Annotations\n'
        'from typing import Annotated'
    ] + [
        to_pydantic_model(sql_stmt) for sql_stmt in sql
    ]
    return '\n\n'.join(to_join)
