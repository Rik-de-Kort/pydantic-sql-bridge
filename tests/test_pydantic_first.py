import textwrap
from typing import Annotated

from pydantic import BaseModel
from pydantic_sql_bridge.pydantic_first import generate_sql, setup_database
from pydantic_sql_bridge.utils import DatabaseType, Annotations
from pydantic_sql_bridge.read_write import cursor, raw_query


class User(BaseModel):
    id: Annotated[int, Annotations.PRIMARY_KEY]
    name: str = "Jane Doe"


class CheckingAccount(BaseModel):
    account_name: Annotated[str, Annotations.PRIMARY_KEY]
    user: int
    balance: float


def test_generate_sql_sqlite():
    # Note: spaces after commas here are very important
    expected = textwrap.dedent(
        """
    CREATE TABLE User (
        id INTEGER NOT NULL PRIMARY KEY, 
        name TEXT NOT NULL
    );
    
    CREATE TABLE CheckingAccount (
        account_name TEXT NOT NULL PRIMARY KEY, 
        user INTEGER NOT NULL, 
        balance REAL NOT NULL
    )
    """
    ).replace("    ", "")
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.SQLITE)
    assert (
        actual.lower().replace("\n", "").strip()
        == expected.lower().replace("\n", "").strip()
    )


def test_generate_sql_mssql():
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.MSSQL)
    print(actual)


def test_setup_database_sqlite():
    with cursor("localhost", ":memory:") as c:
        setup_database(c, [User, CheckingAccount])
        db_schema = {r["name"]: r for r in raw_query(c, "PRAGMA table_list")}
        assert {"name": "User", "ncol": 2, "type": "table"}.items() <= db_schema[
            "User"
        ].items()
        assert {
            "name": "CheckingAccount",
            "ncol": 3,
            "type": "table",
        }.items() <= db_schema["CheckingAccount"].items()
