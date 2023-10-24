import textwrap

from pydantic_sql_bridge.sql_first import create_models_from_sql


def test_generate_models():
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

    expected = textwrap.dedent('''
    from pydantic import BaseModel

    class PortfolioRow(BaseModel):
        sedol: str
        cluster: str
        n_invested: int

    class BenchmarkRow(BaseModel):
        sedol: str
        name: str
        n_available: int
        is_reit: bool
    ''')
    actual = create_models_from_sql([portfolio_sql, benchmark_sql])
    assert actual.replace('\n', '').strip() == expected.replace('\n', '').strip()
