from typing import Annotated

from pydantic import BaseModel
from sqlglot import expressions as exp

from pydantic_sql_bridge.read_write import cursor, get_where, write, build_query
from pydantic_sql_bridge.pydantic_first import setup_database
from pydantic_sql_bridge.utils import Annotations


class Trade(BaseModel):
    id: Annotated[int, Annotations.PRIMARY_KEY]
    counterparty: int
    amount: float


def test_roundtrip_simple():
    transactions = [
        Trade(id=0, counterparty=1, amount=1.0),
        Trade(id=1, counterparty=0, amount=2.0),
    ]
    with cursor(':memory:') as c:
        setup_database(c, [Trade])
        write(c, transactions)
        result = get_where(c, Trade)
    assert len(result) == 2 and transactions[0] in result and transactions[1] in result


def test_build_query():
    expected = exp.select('Trade.id', 'Trade.counterparty', 'Trade.amount').from_('Trade')
    assert build_query(Trade) == expected