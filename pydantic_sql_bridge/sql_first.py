from pathlib import Path

from sqlglot import transpile, Dialects, parse_one, expressions as expr

from pydantic_sql_bridge.read_write import cursor

portfolio_sql = '''CREATE TABLE Portfolio (
    sedol NCHAR(7) PRIMARY KEY,
    cluster NVARCHAR(50),
    n_invested BIGINT
)'''

benchmark_sql = '''CREATE TABLE Benchmark (
    sedol NCHAR(7),
    name NVARCHAR(50),
    n_available BIGINT,
    is_reit BIT,
    CONSTRAINT FK_Sedol FOREIGN KEY (sedol) REFERENCES portfolio(sedol)
)'''

portfolio_sqlite = transpile(portfolio_sql, Dialects.TSQL, Dialects.SQLITE)[0]
benchmark_sqlite = transpile(benchmark_sql, Dialects.TSQL, Dialects.SQLITE)[0]

pf_expr = parse_one(portfolio_sqlite)
table_name = pf_expr.this.this.this.this

SqlglotType = expr.DataType.Type
SQLGLOT_TYPE_TO_PYDANTIC = {
    SqlglotType.INT: int,
    SqlglotType.TEXT: str,
    SqlglotType.BIT: bool,
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


print(to_pydantic_model(portfolio_sqlite))
print(to_pydantic_model(benchmark_sqlite))

Path('test.db').unlink(missing_ok=True)
with cursor('test.db') as c:
    c.execute(portfolio_sqlite)
    c.execute(benchmark_sqlite)
