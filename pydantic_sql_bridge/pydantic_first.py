from textwrap import indent
from typing import Type

from pydantic import BaseModel
from sqlglot import transpile, Dialects

from pydantic_sql_bridge.utils import DatabaseType, get_database_type, Cursor


def get_table_name(typ: type) -> str:
    return typ.__name__[-3] if typ.__name__.endswith('Row') else typ.__name__


def translate_type(typ: type) -> str:
    if typ == int:
        return 'integer not null'
    elif typ == str:
        return 'text not null'
    elif typ == float:
        return 'real not null'
    raise TypeError(f'Unknown type {typ}')


def get_foreign_key(typ: type) -> (str, type):
    fk_table = get_table_name(typ)
    if 'id' in typ.__fields__:
        fk_type = translate_type(typ.__fields__['id'].type_)
    else:
        fk_type = translate_type(int)
    return fk_table, f'{fk_table}_id', fk_type


def generate_sql(models: list[Type[BaseModel]], database_type: DatabaseType) -> str:
    create_stmts = []
    for model in models:
        table_name = get_table_name(model)
        columns = []
        foreign_keys = []
        for field in model.__fields__.values():
            if issubclass(field.type_, BaseModel):
                fk_table, fk_name, fk_type = get_foreign_key(model)
                columns.append(f'{fk_name} {fk_type}')
                foreign_keys.append(f'FOREIGN KEY ({fk_name}) REFERENCES {fk_table}(id)')
            else:
                columns.append(f'{field.name} {translate_type(field.type_)}')

        if foreign_keys:
            columns.extend(foreign_keys)
        columns = ',\n'.join(columns)
        create_stmts.append(f'CREATE TABLE {table_name} (\n{indent(columns, "  ")}\n)')
    return ';\n\n'.join(transpile(';\n\n'.join(create_stmts), Dialects.SQLITE, database_type.value))


def setup_database(c: Cursor, models: list[Type[BaseModel]]):
    db_type = get_database_type(c)
    sql = generate_sql(models, database_type=db_type)
    for create_table in sql.split('\n\n'):
        c.execute(create_table)
