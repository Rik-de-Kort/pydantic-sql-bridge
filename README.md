# Pydantic SQL Bridge 🌉

SQL and Pydantic models, simplified. Get the benefits of developing with Pydantic while harnessing all the power your SQL database has to offer. You can read and write to most SQL databases like SQLite, PostgreSQL, MSSQL, and
MySQL.

Pydantic-SQL-bridge generates Pydantic models for your database tables and the queries you write using those tables. It allows you to write type-safe Python and use query results in FastAPI apps without having to repeat your SQL schema in Python.

Pydantic-SQL-bridge can also translate your Pydantic models into SQL code, allowing you to easily spin up a new database. It will grow with your database usage, like when you start writing optimized queries. 

## Installation

Pydantic-SQL-bridge is available on PyPI.

```shell
pip install pydantic-sql-bridge
```

## How to use

There are two options for using Pydantic-SQL-bridge: SQL first, or Pydantic first.

### SQL first

Use this if are generating your Pydantic models based on your database, for instance if someone else is maintaining the database. The primary way is to derive the models from the database directly, like so:

```python
from pydantic_sql_bridge.utils import cursor
from pydantic_sql_bridge.sql_first import create_models_from_db

with cursor('local', 'sqlite') as c, open('models.py', 'w+') as handle:
    handle.write('# GENERATED FILE')
    handle.write(create_models_from_db(c))
```

Pydantic-SQL-bridge will generate a Python file that you can write to a location of your choosing. You can check this into your repo and get all the benefits of developing with Pydantic. If you have a repository of SQL statements that define your database schema, you can also read use that to generate the models.

```python
from pydantic_sql_bridge.sql_first import create_models_from_sql

with open('table_definitions.sql', 'r') as handle:
    sql = handle.read().split('\n\n')

with open('models.py') as handle:
    handle.write(create_models_from_sql(sql))
```

By default, Pydantic-SQL-bridge will generate models for all your tables. Support for views and arbitrary select queries is planned.

### Pydantic first

Use this if you are setting up a new database.

To setup a database according to our Pydantic models, we import `cursor` and `setup_database`.

```python
from pydantic import BaseModel
from pydantic_sql_bridge.utils import cursor
from pydantic_sql_bridge.pydantic_first import setup_database


class User(BaseModel):
    id: int
    name = 'Jane Doe'


class CheckingAccount(BaseModel):
    user: User
    balance: float


with cursor('local', 'sqlite') as c:
    setup_database(c, [User, CheckingAccount])
    c.connection.commit()
```

If you prefer to generate SQL to setup your database (for instance, if you are deploying the database separately, or you want to make manual adjustments), we can use `generate_sql`. Since we are not connecting to a database directly, we'll also have to tell Pydantic-SQL-bridge what`DatabaseType` you are using.

```python
from pydantic import BaseModel
from pydantic_sql_bridge.pydantic_first import generate_sql
from pydantic_sql_bridge.utils import DatabaseType


class User(BaseModel):
    id: int
    name = 'Jane Doe'


class CheckingAccount(BaseModel):
    user: User
    balance: float


sql = generate_sql([User, CheckingAccount], database_type=DatabaseType.SQLITE)
with open('table_definitions.sql', 'w+') as handle:
    handle.write(sql)
```

## Notes from the maintainers

### Nested models

Pydantic-SQL-bridge does not support directly writing nested models to and reading them from your database: it encourages you to work more directly with the database and the capabilities it has to offer. It does offer utilities for nesting and un-nesting models.

```python
from pydantic import BaseModel
from pydantic_sql_bridge.nested import flatten, split


class Trade(BaseModel):
    id: int
    counterparty: str
    amount: float


class CheckingAccount(BaseModel):
    id: int
    name: str
    balance: float
    last_transaction: Trade


class FlattenedCheckingAccount(BaseModel):
    id: int
    name: str
    balance: float
    last_transaction_id: int
    last_transaction_counterparty: str
    last_transaction_amount: float


class SplitCheckingAccount(BaseModel):
    id: int
    name: str
    balance: float
    last_transaction_id: int


trade = Trade(id=0, counterparty='Alice', amount=-5)
bobs_account = CheckingAccount(id=1, name='Bob', balance=100, last_transaction=trade)

bobs_flattened_account = FlattenedCheckingAccount(
    id=1, name='Bob', balance=100,
    last_transaction_id=0, last_transaction_counterparty='Alice', last_transaction_amount=-5
)
assert bobs_flattened_account == flatten(bobs_account)

bobs_split_account = SplitCheckingAccount(
    id=1, name='Bob', balance=100,
    last_transaction_id=0
)
assert bobs_split_account, trade == split(bobs_account, primary_key='id')
```

Flatten seems pretty stupid tbh.

"ORM" implies taking on object-oriented programming features like inheritance. This does not match with the database
model, which is about sets of records, and relations between them. These paradigms don't match, and I think trying to
map them (ORM stands for "Object-Relational Mapper") is a mistake.
See [here](https://blog.codinghorror.com/object-relational-mapping-is-the-vietnam-of-computer-science/) for an in-depth
explanation.
