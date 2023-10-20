from sqlglot import parse_one, expressions as expr

SqlglotType = expr.DataType.Type
SQLGLOT_TYPE_TO_PYDANTIC = {
    SqlglotType.INT: int,
    SqlglotType.TEXT: str,
    SqlglotType.BIT: bool,
    SqlglotType.NCHAR: str,
    SqlglotType.NVARCHAR: str,
    SqlglotType.BIGINT: int,
}


def transform_column_def(col_def: expr.ColumnDef) -> tuple[str, type]:
    name = col_def.this.this
    pydantic_type = SQLGLOT_TYPE_TO_PYDANTIC[col_def.args['kind'].this]
    return name, pydantic_type


def parse_create_table(sql_stmt) -> tuple[str, list[tuple[str, type]]]:
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
    head = f'class {table_name.capitalize()}(BaseModel):\n'
    body = '\n'.join(f'    {name}: {typ.__name__}' for name, typ in column_defs)
    result = head + body
    return result


def create_models_from_sql(sql: list[str]) -> str:
    models = [to_pydantic_model(sql_stmt) for sql_stmt in sql]
    head = 'from pydantic import BaseModel'
    return '\n\n'.join([head] + models)
