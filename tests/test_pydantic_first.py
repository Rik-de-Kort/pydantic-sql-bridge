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
    # Note: spaces after commas here are very important
    expected = textwrap.dedent('''
    CREATE TABLE User (
        __psb_id__ INTEGER NOT NULL, 
        id INTEGER NOT NULL, 
        name TEXT NOT NULL, 
        PRIMARY KEY (__psb_id__)
    );
    
    CREATE TABLE CheckingAccount (
        __psb_id__ INTEGER NOT NULL, 
        CheckingAccount_id INTEGER NOT NULL, 
        balance REAL NOT NULL, 
        PRIMARY KEY (__psb_id__), 
        FOREIGN KEY (CheckingAccount_id) REFERENCES CheckingAccount(__psb_id__)
    )
    ''').replace('    ', '')
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.SQLITE)
    assert expected.lower().replace('\n', '').strip() == actual.lower().replace('\n', '').strip()


def test_generate_sql_mssql():
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.MSSQL)
    print(actual)


def test_setup_database_sqlite():
    with cursor(':memory:') as c:
        setup_database(c, [User, CheckingAccount])
        db_schema = {r['name']: r for r in query(c, 'PRAGMA table_list')}
        assert {'name': 'User', 'ncol': 3, 'type': 'table'}.items() <= db_schema['User'].items()
        assert {'name': 'CheckingAccount', 'ncol': 3, 'type': 'table'}.items() <= db_schema['CheckingAccount'].items()
