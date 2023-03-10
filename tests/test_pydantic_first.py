import textwrap

from pydantic import BaseModel
from pydantic_sql_bridge.pydantic_first import generate_sql, setup_database
from pydantic_sql_bridge.utils import DatabaseType, cursor, query


class User(BaseModel):
    id: int
    name = 'Jane Doe'


class CheckingAccount(BaseModel):
    user: User
    balance: float


def test_generate_sql_sqlite():
    expected = 'CREATE TABLE User (id INTEGER NOT NULL, name TEXT NOT NULL);\n\nCREATE TABLE CheckingAccount (CheckingAccount_id INTEGER NOT NULL, balance REAL NOT NULL, FOREIGN KEY (CheckingAccount_id) REFERENCES CheckingAccount(id))'
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.SQLITE)
    assert expected.lower().strip() == actual.lower().strip()


def test_setup_database_sqlite():
    with cursor(':memory:') as c:
        setup_database(c, [User, CheckingAccount])
        db_schema = {r['name']: r for r in query(c, 'PRAGMA table_list')}
        assert {'name': 'User', 'ncol': 2, 'type': 'table'}.items() <= db_schema['User'].items()
        assert {'name': 'CheckingAccount', 'ncol': 2, 'type': 'table'}.items() <= db_schema['CheckingAccount'].items()
