from pydantic import BaseModel
import pytest

from pydantic_sql_bridge.read_write import cursor, get_where, write, build_query, SelectQuery, InnerJoin
from pydantic_sql_bridge.pydantic_first import setup_database


class Transaction(BaseModel):
    __id__ = ('id',)
    id: int
    counterparty: int
    amount: float


class CheckingAccount(BaseModel):
    __id__ = ('id',)
    id: int
    balance: float
    last_transaction: Transaction


class User(BaseModel):
    id: int
    name: str
    checking_account: CheckingAccount


def test_roundtrip_simple():
    transactions = [
        Transaction(id=0, second_to_last_transaction=1.0),
        Transaction(id=1, second_to_last_transaction=2.0),
    ]
    with cursor(':memory:') as c:
        setup_database(c, [Transaction, CheckingAccount])
        write(c, transactions)
        result = get_where(c, Transaction)
    assert len(result) == 2 and transactions[0] in result and transactions[1] in result


@pytest.mark.xfail
def test_roundtrip_fk():
    transactions = [
        Transaction(id=0, second_to_last_transaction=1.0),
        Transaction(id=1, second_to_last_transaction=2.0),
    ]
    checking_accounts = [
        CheckingAccount(id=0, transaction_history=transactions[0], balance=1000),
        CheckingAccount(id=1, transaction_history=transactions[1], balance=1500),
    ]
    with cursor(':memory:') as c:
        setup_database(c, [Transaction, CheckingAccount])
        write(c, transactions)
        write(c, checking_accounts)
        result = get_where(c, CheckingAccount)
    assert len(result) == 2 and checking_accounts[0] in checking_accounts and checking_accounts[1] in checking_accounts


def test_build_query():
    expected = SelectQuery(
        columns={('Transaction', 'id'), ('Transaction', 'counterparty'), ('Transaction', 'amount')},
        from_table='Transaction',
        inner_joins=[],
    )
    assert build_query(Transaction) == expected

    expected = SelectQuery(
        columns={
            ('CheckingAccount', 'id'), ('CheckingAccount', 'balance'),
            ('Transaction', 'id'), ('Transaction', 'counterparty'), ('Transaction', 'amount')
        },
        from_table='CheckingAccount',
        inner_joins=[
            InnerJoin(left='CheckingAccount', right='Transaction', on=[('Transaction_id', 'id')])
        ],
    )
    assert build_query(CheckingAccount) == expected

    expected = SelectQuery(
        columns={
            ('User', 'id'), ('User', 'name'),
            ('CheckingAccount', 'id'), ('CheckingAccount', 'balance'),
            ('Transaction', 'id'), ('Transaction', 'counterparty'), ('Transaction', 'amount')
        },
        from_table='User',
        inner_joins=[
            InnerJoin(left='User', right='CheckingAccount', on=[('CheckingAccount_id', 'id')]),
            InnerJoin(left='CheckingAccount', right='Transaction', on=[('Transaction_id', 'id')])
        ],
    )
    assert build_query(User) == expected
