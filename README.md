# Pydantic SQL Bridge 🌉

SQL and Pydantic models, simplified. Maintain straightforward one-to-one mappings between database tables and Pydantic
models, without the baggage of an ORM. You can read and write to most SQL databases like SQLite, PostgreSQL, MSSQL, and
MySQL.

## Installation

Pydantic-SQL-bridge is available on PyPI.

```shell
pip install pydantic-sql-bridge
```

## How to use

There are two options for using Pydantic-SQL-bridge: Pydantic first, or SQL first.

### Pydantic first

Use this if you are setting up a new database, and want to control your database schema from your Pydantic models.

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

If you prefer to generate SQL to setup your database (for instance, if you are deploying the database separately, or you
want to make manual adjustments), we can use `generate_sql`. Since we are not connecting to a database directly, we'll
also have to tell Pydantic-SQL-bridge what`DatabaseType` you are using.

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

### SQL first
Note: this feature is not available yet.

Use this if you prefer to generate your Pydantic models based on your database, for instance if someone else is
maintaining the database. The primary way is to derive the models from the database directly, like so:

```python
from pydantic_sql_bridge.utils import cursor
from pydantic_sql_bridge.sql_first import create_models_from_db

with cursor('local', 'sqlite') as c:
    create_models_from_db(c, filename='models.py')
```

Pydantic-SQL-bridge will generate a `models.py` file in your current directory (you can of course modify the
filename/path). You can check this into your repo and get all the benefits of developing with Pydantic.

If you have a repository of SQL statements that define your database schema, you can also read use that to generate the
models.

```python
from pydantic_sql_bridge.sql_first import create_models_from_sql

with open('table_definitions.sql', 'r') as handle:
    sql = handle.read().split('\n\n')

with open('models.py') as handle:
    handle.write(create_models_from_sql(sql))
```

## Notes from the maintainers

### Nested models

```python
from pydantic import BaseModel


class Trade(BaseModel):
    __id__ = ('id',)
    id: int
    counterparty: int
    amount: float


class CheckingAccount(BaseModel):
    id: int
    balance: float
    last_transaction: Trade
```

If you have nested models, Pydantic-SQL-bridge will put the models in separate tables, and retrieve them as needed.
It is necessary to tell Pydantic-SQL-bridge which fields identify the nested model. You do this by setting the `__id__`
attribute.

This feature is still in development. Pydantic-SQL-bridge already gets nested models correctly from the database,
but writing still has to be done per model.
The `__id__` attribute is also not so nice to use. In the future Pydantic-SQL-bridge will include helpers to make this
easier.

The feature is a bit ORM-y in the sense that Pydantic-SQL-Bridge stops being a very thin wrapper and the reality of the SQL database. However, if you have a normalized schema, it will be quite common to want to work with data that's returned from a join rather than the individual tables themselves. Or you could just get the data from a view which does the joining, in which case the data is flat, but flat is better than nested anyway.

2023-10-20 status: remove foreign key features

## Why not an ORM?

"ORM" implies taking on object-oriented programming features like inheritance. This does not match with the database
model, which is about sets of records, and relations between them. These paradigms don't match, and I think trying to
map them (ORM stands for "Object-Relational Mapper") is a mistake.
See [here](https://blog.codinghorror.com/object-relational-mapping-is-the-vietnam-of-computer-science/) for an in-depth
explanation.
