# Pydantic SQL Bridge 🌉

SQL and Pydantic models, simplified. Get the benefits of developing with Pydantic while harnessing all the power your
SQL database has to offer. You can read and write to most SQL databases like SQLite, PostgreSQL, MSSQL, and
MySQL.

Pydantic-SQL-bridge generates Pydantic models for your database tables and the queries you write using those tables. It
allows you to write type-safe Python and use query results in FastAPI apps without having to re-write your SQL schema in
Python.

Pydantic-SQL-bridge can also translate your Pydantic models into SQL code, allowing you to easily spin up a new
database. It will grow with your database usage, like when you start writing optimized SQL queries.

## Installation

Pydantic-SQL-bridge is available on PyPI.

```shell
pip install pydantic-sql-bridge
```

## How to use
### Example

We set up a SQL table for portfolios and associated benchmark data.

```sql
CREATE TABLE Portfolio (
    sedol NCHAR(7) PRIMARY KEY,
    cluster NVARCHAR(50),
    n_invested BIGINT
)

CREATE TABLE Benchmark (
    sedol NCHAR(7),
    name NVARCHAR(50),
    n_available BIGINT,
    is_reit BIT,
    CONSTRAINT FK_Sedol FOREIGN KEY (sedol) REFERENCES portfolio(sedol)
)
```

For this schema, Pydantic-SQL-bridge generates the following Python file.

```python
# models.py
from pydantic import BaseModel
from typing import Annotated
from pydantic_sql_bridge.utils import Annotations


class PortfolioRow(BaseModel):
    sedol: Annotated[str, Annotations.PRIMARY_KEY]
    cluster: str
    n_invested: int


class BenchmarkRow(BaseModel):
    sedol: str
    name: str
    n_available: int
    is_reit: bool
```

You can then write to and query from the database as follows.

```python
from pydantic_sql_bridge.read_write import cursor, get_where, write
from models import BenchmarkRow, PortfolioRow

with cursor('localhost', ':memory:') as c:
    write(c, [BenchmarkRow(sedol='AAAAAAA', name='Test', n_available=14, is_reit=False)], compare_on=('sedol',),
          should_insert=True, should_update=True, should_delete=False)
    benchmark = get_where(c, BenchmarkRow)
    eu_retail_portfolio = get_where(c, PortfolioRow, cluster='Europe Retail')
```

### Generating Pydantic models

If you have a repository of SQL statements that define your database schema, use `create_models_from_sql` to
get the source code of a Python file with Pydantic models. You can check it into your repo to get all
the benefits of working with Pydantic.

```python
from pydantic_sql_bridge.sql_first import create_models_from_sql

with open('table_definitions.sql', 'r') as handle:
    sql = handle.read().split('\n\n')

with open('models.py') as handle:
    handle.write(create_models_from_sql(sql))
```

By default, Pydantic-SQL-bridge will generate models for all your tables and views. Support for arbitrary select queries
is planned.

You can also  derive the models from the database directly, like so:

```python
from pydantic_sql_bridge.read_write import cursor
from pydantic_sql_bridge.sql_first import create_models_from_db

with cursor('local', 'sqlite') as c, open('models.py', 'w+') as handle:
    handle.write('# GENERATED FILE')
    handle.write(create_models_from_db(c))
```

Pydantic-SQL-bridge adds a special class variable to the generated models called `query_name`. This is how it knows
which table or view to query.

### Pydantic first

Use this if you are setting up a new database.

To set up a database according to our Pydantic models, we import `cursor` and `setup_database`.

```python
from pydantic import BaseModel
from pydantic_sql_bridge.read_write import cursor
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

If you prefer to generate SQL to set up your database (for instance, if you are deploying the database separately, or
you want to make manual adjustments), we can use `generate_sql`. Since we are not connecting to a database directly,
we'll also have to tell Pydantic-SQL-bridge what`DatabaseType` you are using.

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

## Notes and remarks

### Why can't I control my database using just Pydantic-SQL-bridge?

SQL is a much older technology than Python (and certainly Pydantic!), and is much more widespread. Pretty much every
programming language has a way of talking with SQL databases, and databases tend to outlive their associated
applications. SQL skills are one of the few things you can invest in for an almost guaranteed benefit, wherever your
software journey takes you. Trying to control the database from Python is rather putting the cart before the horse.

It doesn't help that Python "things" are *objects*, which you can nest (like when you
have [`Foo` as an attribute of `Spam`](https://docs.pydantic.dev/latest/concepts/models/#nested-models)), and which
can "do stuff" ( like when you call `model.model_dump()`).  "Things" in SQL databases are *relations*, which you cannot
nest, and which cannot do stuff (they are "just data"), so you cannot easily translate between those two worlds. There
do exist packages (called Object-Relational-Mappers or ORM's) that try to let you do this, such
as [SQLAlchemy](https://www.sqlalchemy.org/). If you go that way, you need to rely on the ORM's maintainers to implement
support for the database features you need, rather than just using the database however you want. And the skills you
learn are not transferable: if your next project is in C#, you cannot use SQLAlchemy.

Pydantic-SQL-bridge's solution is to start from SQL and adapt our Python code around it. Of course we help you get
started using just Python, but these are training wheels. If you need something different from your database, you have
the chance to learn some SQL, and we will help you make sense of it on the Python end.

### Nested models

Pydantic-SQL-bridge does not support directly writing nested models to and reading them from your database: it
encourages you to work more directly with the database and the capabilities it has to offer. It does offer a utility for
nesting and unnesting models, to more easily translate between your application's models and the ones generated by
Pydantic-SQL-bridge.

```python
from pydantic import BaseModel
from typing import ClassVar

from pydantic_sql_bridge.read_write import cursor, get_where, write
from pydantic_sql_bridge.utils import transform


class First(BaseModel):
    id: int
    name: str


class Second(BaseModel):
    id: int
    score: float


class Nested(BaseModel):
    id: int
    first: First
    second: Second


class Flat(BaseModel):
    query_name: ClassVar[str] = 'example'
    id: int
    first_id: int
    first_name: str
    second_id: int
    second_score: float


targets = [Nested(id=0, first=First(id=0, name='alice'), second=Second(id=1, score=-5.21)),
           Nested(id=1, first=First(id=1, name='bob'), second=Second(id=2, score=348.7))]

with cursor('localhost', ':memory:') as c:
    write(c, [transform(target, Flat) for target in targets])
    query_result = get_where(c, Flat)

targets: list[Nested] = [transform(r, Nested) for r in query_result]
```
