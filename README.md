# Pydantic SQL Bridge 🌉
SQL and Pydantic models, simplified. Maintain straightforward one-to-one mappings between database tables and Pydantic models, without the baggage of an ORM. You can read and write to most SQL databases like SQLite, PostgreSQL, MSSQL, and MySQL.

## How to use
As always, first we install.

```shell
pip install pydantic-sql-bridge
```

There are two options for using Pydantic-SQL-bridge: Pydantic first, or SQL first.

### Pydantic first
Use this if you are setting up a new database, and want to control your database schema from your Pydantic models.

To setup a database according to our Pydantic models, we import `cursor` and `setup_database`.

```python
from pydantic import BaseModel
from pydantic_sql_bridge import cursor, setup_database


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
from pydantic_sql_bridge import generate_sql, DatabaseType


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
Use this if you prefer to generate your Pydantic models based on your database, for instance if someone else is maintaining the database. The primary way is to derive the models from the database directly, like so:

```python
from pydantic_sql_bridge import cursor, create_models_from_db

with cursor('local', 'sqlite') as c:
    create_models_from_db(c, filename='models.py')
```

Pydantic-SQL-bridge will generate a `models.py` file in your current directory (you can of course modify the filename/path). You can check this into your repo and get all the benefits of developing with Pydantic. 

If you have a repository of SQL statements that define your database schema, you can also read use that to generate the models.

```python
from pydantic_sql_bridge import create_models_from_sql

with open('table_definitions.sql', 'r') as handle:
    sql = handle.read()

create_models_from_sql(sql, filename='models.py')
```


## Why not an ORM?
"ORM" implies taking on object-oriented programming features like inheritance. This does not match with the database model, which is about sets of records, and relations between them. These paradigms don't match, and I think trying to map them (ORM stands for "Object-Relational Mapper") is a mistake. See [here](https://blog.codinghorror.com/object-relational-mapping-is-the-vietnam-of-computer-science/) for an in-depth explanation.
