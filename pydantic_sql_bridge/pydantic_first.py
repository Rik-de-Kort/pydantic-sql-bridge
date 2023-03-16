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


def get_fk_types(typ: Type[BaseModel]) -> tuple:
    if not hasattr(typ, '__id__'):
        raise TypeError(f'Model {typ} needs an __id__ attribute in order to be used as a foreign key.')
    if any(not_present := [name for name in typ.__id__ if name not in typ.__fields__]):
        raise TypeError(f'Key names {not_present} specified in __id__ are not present on {typ}.')
    if any(references := [name for name in typ.__id__ if issubclass(typ.__fields__[name].type_, BaseModel)]):
        raise TypeError(f'Cannot specify {references} in __id__ because they are Pydantic models.')
    key_types = tuple(translate_type(typ.__fields__[name].type_) for name in typ.__id__)
    return key_types


def generate_sql(models: list[Type[BaseModel]], database_type: DatabaseType) -> str:
    create_stmts = []
    for model in models:
        table_name = get_table_name(model)
        columns = []
        constraints = []
        if hasattr(model, '__id__'):
            if any(not_found := [name for name in model.__id__ if name not in model.__fields__]):
                raise TypeError(f'Fields {not_found} from __id__ not found on model {model}')
            constraints.append(f'PRIMARY KEY ({", ".join(model.__id__)})')

        for field in model.__fields__.values():
            if issubclass(field.type_, BaseModel):
                fk_table = get_table_name(field.type_)
                fk_types = get_fk_types(field.type_)
                fk_names = [f'{fk_table}_{name}' for name in field.type_.__id__]
                for name, typ in zip(fk_names, fk_types):
                    columns.append(f'{name} {typ}')

                fk_names = ', '.join(fk_names)
                referred_names = ', '.join(field.type_.__id__)
                constraints.append(f'FOREIGN KEY ({fk_names}) REFERENCES {fk_table}({referred_names})')
            else:
                columns.append(f'{field.name} {translate_type(field.type_)}')

        # Note: constraints always has at least one entry
        columns.extend(constraints)
        columns = ',\n'.join(columns)
        create_stmts.append(f'CREATE TABLE {table_name} (\n{indent(columns, "  ")}\n)')
    return ';\n\n'.join(transpile(';\n\n'.join(create_stmts), Dialects.SQLITE, database_type.value))


def setup_database(c: Cursor, models: list[Type[BaseModel]]):
    db_type = get_database_type(c)
    sql = generate_sql(models, database_type=db_type)
    for create_table in sql.split('\n\n'):
        c.execute(create_table)
