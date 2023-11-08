import textwrap

from sqlglot import parse_one, Dialects

from pydantic_sql_bridge.sql_first import create_models_from_sql, parse_create_table


def test_generate_models_tsql():
    sql = '''
    CREATE TABLE "test" ("name" CHAR (7) NOT NULL, "internal_id" VARCHAR(10) NOT NULL, UNIQUE NONCLUSTERED ("internal_id" ASC))
    '''
    expected = textwrap.dedent('''
    from pydantic import BaseModel
    from pydantic_sql_bridge.utils import Annotations
    from typing import Annotated, ClassVar, Optional

    class TestRow(BaseModel):
        query_name: ClassVar[str] = "test"
        name: str
        internal_id: str
    ''')
    actual = create_models_from_sql([sql], dialect=Dialects.TSQL)
    assert actual.replace('\n', '').strip() == expected.replace('\n', '').strip()


def test_generate_models_straightforward():
    portfolio_sql = '''CREATE TABLE Portfolio (
        sedol NCHAR(7) PRIMARY KEY,
        cluster NVARCHAR(50) NULL,
        n_invested BIGINT
        CONSTRAINT FK_Sedol FOREIGN KEY (sedol) REFERENCES benchmark(sedol)
    )'''

    benchmark_sql = '''CREATE TABLE Benchmark (
        sedol NCHAR(7) PRIMARY KEY,
        name NVARCHAR(50),
        n_available BIGINT,
        is_reit BIT,
    )'''

    master_sql = '''CREATE VIEW master AS
    SELECT p.sedol, b.name, p.n_invested, b.n_available
    FROM Portfolio p
    JOIN Benchmark b ON p.sedol = b.sedol
    '''

    expected = textwrap.dedent('''
    from pydantic import BaseModel
    from pydantic_sql_bridge.utils import Annotations
    from typing import Annotated, ClassVar, Optional

    class PortfolioRow(BaseModel):
        query_name: ClassVar[str] = "Portfolio"
        sedol: Annotated[str, Annotations.PRIMARY_KEY]
        cluster: Optional[str]
        n_invested: int

    class BenchmarkRow(BaseModel):
        query_name: ClassVar[str] = "Benchmark"
        sedol: Annotated[str, Annotations.PRIMARY_KEY]
        name: str
        n_available: int
        is_reit: bool
        
    class MasterRow(BaseModel):
        query_name: ClassVar[str] = "master"
        sedol: str
        name: str
        n_invested: int
        n_available: int
    ''')
    actual = create_models_from_sql([portfolio_sql, benchmark_sql, master_sql])
    assert actual.replace('\n', '').strip() == expected.replace('\n', '').strip()
