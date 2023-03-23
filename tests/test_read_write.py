from pydantic import BaseModel
import pytest
from sqlglot import expressions as exp

from pydantic_sql_bridge.read_write import cursor, get_where, write, build_query
from pydantic_sql_bridge.pydantic_first import setup_database


class Trade(BaseModel):
    __id__ = ('id',)
    id: int
    counterparty: int
    amount: float


class CheckingAccount(BaseModel):
    __id__ = ('id',)
    id: int
    balance: float
    last_transaction: Trade


class User(BaseModel):
    id: int
    name: str
    checking_account: CheckingAccount


def test_roundtrip_simple():
    transactions = [
        Trade(id=0, counterparty=1, amount=1.0),
        Trade(id=1, counterparty=0, amount=2.0),
    ]
    with cursor(':memory:') as c:
        setup_database(c, [Trade, CheckingAccount])
        write(c, transactions)
        result = get_where(c, Trade)
    assert len(result) == 2 and transactions[0] in result and transactions[1] in result


def test_roundtrip_fk():
    transactions = [
        Trade(id=0, counterparty=1, amount=1.0),
        Trade(id=1, counterparty=0, amount=2.0),
    ]
    checking_accounts = [
        CheckingAccount(id=0, last_transaction=transactions[0], balance=1000),
        CheckingAccount(id=1, last_transaction=transactions[1], balance=1500),
    ]
    with cursor(':memory:') as c:
        setup_database(c, [Trade, CheckingAccount])
        write(c, transactions)
        write(c, checking_accounts)
        result = get_where(c, CheckingAccount)
    assert len(result) == 2 and checking_accounts[0] in checking_accounts and checking_accounts[1] in checking_accounts


def test_build_query():
    expected = exp.select('Trade.id', 'Trade.counterparty', 'Trade.amount').from_('Trade')
    assert build_query(Trade) == expected

    expected = exp.select(
        'CheckingAccount.id', 'CheckingAccount.balance',
        'Trade.id', 'Trade.counterparty', 'Trade.amount'
    ).from_(
        'CheckingAccount'
    ).join(
        'Trade', on='CheckingAccount.Trade_id = Trade.id', join_type='inner'
    )
    assert build_query(CheckingAccount) == expected

    expected = exp.select(
        'User.id', 'User.name',
        'CheckingAccount.id', 'CheckingAccount.balance',
        'Trade.id', 'Trade.counterparty', 'Trade.amount'
    ).from_(
        'User'
    ).join(
        'CheckingAccount', on='User.CheckingAccount_id = CheckingAccount.id', join_type='inner'
    ).join(
        'Trade', on='CheckingAccount.Trade_id = Trade.id', join_type='inner'
    )
    assert build_query(User) == expected
