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
    expected = textwrap.dedent('''
        CREATE TABLE User (
          id integer not null,
          name text not null
        )
        
        CREATE TABLE CheckingAccount (
          CheckingAccount_id integer not null,
          balance real not null,
          FOREIGN KEY (CheckingAccount_id) REFERENCES CheckingAccount(id)
        )''')
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.SQLITE)
    assert expected.lower().strip() == actual.lower().strip()


def test_setup_database_sqlite():
    with cursor(':memory:') as c:
        setup_database(c, [User, CheckingAccount])
        db_schema = {r['name']: r for r in query(c, 'PRAGMA table_list')}
        assert {'name': 'User', 'ncol': 2, 'type': 'table'}.items() <= db_schema['User'].items()
        assert {'name': 'CheckingAccount', 'ncol': 2, 'type': 'table'}.items() <= db_schema['CheckingAccount'].items()


