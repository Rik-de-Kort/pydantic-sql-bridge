from pydantic import BaseModel
import pytest

from pydantic_sql_bridge.read_write import cursor, get_where, write
from pydantic_sql_bridge.pydantic_first import setup_database


class User(BaseModel):
    __id__ = ('id',)
    id: int
    name = 'Jane Doe'


class CheckingAccount(BaseModel):
    __id__ = ('account_name',)
    account_name: str
    user: User
    balance: float


def test_roundtrip_simple():
    users = [User(id=0, name='Rik'), User(id=1, name='Alexander')]
    with cursor(':memory:') as c:
        setup_database(c, [User, CheckingAccount])
        write(c, users)
        result = get_where(c, User)
    assert len(result) == 2 and users[0] in result and users[1] in result


@pytest.mark.xfail
def test_roundtrip_fk():
    users = [User(id=0, name='Rik'), User(id=1, name='Alexander')]
    checking_accounts = [
        CheckingAccount(account_name='rik', user=users[0], balance=1000),
        CheckingAccount(account_name='alex', user=users[1], balance=1500),
    ]
    with cursor(':memory:') as c:
        setup_database(c, [User, CheckingAccount])
        write(c, users)
        write(c, checking_accounts)
        result = get_where(c, CheckingAccount)
    assert len(result) == 2 and checking_accounts[0] in checking_accounts and checking_accounts[1] in checking_accounts
