from textwrap import indent
from typing import Type

from pydantic import BaseModel
from sqlglot import transpile, Dialects

from pydantic_sql_bridge.utils import (
    DatabaseType,
    get_database_type,
    Cursor,
    get_table_name,
    get_primary_key,
)


def translate_type(typ: type) -> str:
    if typ == int:
        return "integer not null"
    elif typ == str:
        return "text not null"
    elif typ == float:
        return "real not null"
    raise TypeError(
        f"{typ} not supported yet, use int, str, float, or another pydantic model"
    )


def generate_sql(models: list[Type[BaseModel]], database_type: DatabaseType) -> str:
    create_stmts = []
    for model in models:
        table_name = get_table_name(model)

        columns = [
            f"{name} {translate_type(field.annotation)}"
            for name, field in model.model_fields.items()
        ]
        if primary_key := get_primary_key(model):  # add primary key constraint
            columns.append(f'PRIMARY KEY ({", ".join(primary_key)})')

        columns = ",\n".join(columns)
        create_stmts.append(f'CREATE TABLE {table_name} (\n{indent(columns, "  ")}\n)')
    return ";\n\n".join(
        transpile(";\n\n".join(create_stmts), Dialects.SQLITE, database_type.value)
    )


def setup_database(c: Cursor, models: list[Type[BaseModel]]):
    db_type = get_database_type(c)
    sql = generate_sql(models, database_type=db_type)
    for create_table in sql.split("\n\n"):
        c.execute(create_table)
