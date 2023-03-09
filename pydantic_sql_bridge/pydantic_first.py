from enum import Enum
from textwrap import indent

from pydantic import BaseModel


class DatabaseType(Enum):
    SQLITE = 'SQLITE'


def get_table_name(typ: type) -> str:
    return model.__name__[-3] if model.__name__.endswith('Row') else model.__name__


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


def generate_sql(models: list[BaseModel], database_type: DatabaseType) -> str:
    result = []
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
        result.append(f'CREATE TABLE {table_name} (\n{indent(columns, "  ")}\n)')
    return '\n\n'.join(result)
