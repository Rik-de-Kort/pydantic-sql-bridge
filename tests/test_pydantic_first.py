import textwrap

import pytest
from pydantic import BaseModel
from pydantic_sql_bridge.pydantic_first import generate_sql, setup_database, get_fk_types
from pydantic_sql_bridge.utils import DatabaseType
from pydantic_sql_bridge.read_write import cursor, raw_query


class User(BaseModel):
    __id__ = ('id',)
    id: int
    name: str = 'Jane Doe'


class CheckingAccount(BaseModel):
    __id__ = ('account_name',)
    account_name: str
    user: User
    balance: float


def test_generate_sql_sqlite():
    # Note: spaces after commas here are very important
    expected = textwrap.dedent('''
    CREATE TABLE User (
        id INTEGER NOT NULL PRIMARY KEY, 
        name TEXT NOT NULL
    );
    
    CREATE TABLE CheckingAccount (
        account_name TEXT NOT NULL PRIMARY KEY, 
        User_id INTEGER NOT NULL, 
        balance REAL NOT NULL, 
        FOREIGN KEY (User_id) REFERENCES User (id)
    )
    ''').replace('    ', '')
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.SQLITE)
    assert actual.lower().replace('\n', '').strip() == expected.lower().replace('\n', '').strip()


def test_generate_sql_mssql():
    actual = generate_sql([User, CheckingAccount], database_type=DatabaseType.MSSQL)
    print(actual)


def test_setup_database_sqlite():
    with cursor(':memory:') as c:
        setup_database(c, [User, CheckingAccount])
        db_schema = {r['name']: r for r in raw_query(c, 'PRAGMA table_list')}
        assert {'name': 'User', 'ncol': 2, 'type': 'table'}.items() <= db_schema['User'].items()
        assert {'name': 'CheckingAccount', 'ncol': 3, 'type': 'table'}.items() <= db_schema['CheckingAccount'].items()


class CheckingAccountNoId(BaseModel):
    account_name: str
    user: User
    balance: float


class CheckingAccountTypoId(BaseModel):
    __id__ = ('acount_name',)
    account_name: str
    user: User
    balance: float


class CheckingAccountModelId(BaseModel):
    __id__ = ('user',)
    account_name: str
    user: User
    balance: float


def test_get_fk_types():
    assert get_fk_types(User) == ('integer not null',)
    assert get_fk_types(CheckingAccount) == ('text not null',)
    with pytest.raises(TypeError):
        get_fk_types(CheckingAccountNoId)
    with pytest.raises(TypeError):
        get_fk_types(CheckingAccountTypoId)
    with pytest.raises(TypeError):
        get_fk_types(CheckingAccountModelId)
