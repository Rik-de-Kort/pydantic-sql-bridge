from textwrap import indent
from typing import Type

from pydantic import BaseModel
from sqlglot import transpile, Dialects

from pydantic_sql_bridge.utils import DatabaseType, get_database_type, Cursor, get_table_name, is_model


def translate_type(typ: type) -> str:
    if typ == int:
        return 'integer not null'
    elif typ == str:
        return 'text not null'
    elif typ == float:
        return 'real not null'
    raise TypeError(f'{typ} not supported yet, use int, str, float, or another pydantic model')


def get_fk_types(typ: Type[BaseModel]) -> tuple:
    if not hasattr(typ, '__id__'):
        raise TypeError(f'Model {typ} needs an __id__ attribute in order to be used as a foreign key.')
    if any(not_present := [name for name in typ.__id__ if name not in typ.model_fields]):
        raise TypeError(f'Key names {not_present} specified in __id__ are not present on {typ}.')
    if any(references := [name for name in typ.__id__ if issubclass(typ.model_fields[name].annotation, BaseModel)]):
        raise TypeError(f'Cannot specify {references} in __id__ because they are Pydantic models.')
    key_types = tuple(translate_type(typ.model_fields[name].annotation) for name in typ.__id__)
    return key_types


def generate_sql(models: list[Type[BaseModel]], database_type: DatabaseType) -> str:
    create_stmts = []
    for model in models:
        table_name = get_table_name(model)
        columns = []
        constraints = []
        if hasattr(model, '__id__'):
            if any(not_found := [name for name in model.__id__ if name not in model.model_fields]):
                raise TypeError(f'Fields {not_found} from __id__ not found on model {model}')
            constraints.append(f'PRIMARY KEY ({", ".join(model.__id__)})')

        for name, field in model.model_fields.items():
            if is_model(field):
                fk_table = get_table_name(field.annotation)
                fk_types = get_fk_types(field.annotation)
                fk_names = [f'{fk_table}_{name}' for name in field.annotation.__id__]
                for name, typ in zip(fk_names, fk_types):
                    columns.append(f'{name} {typ}')

                fk_names = ', '.join(fk_names)
                referred_names = ', '.join(field.annotation.__id__)
                constraints.append(f'FOREIGN KEY ({fk_names}) REFERENCES {fk_table}({referred_names})')
            else:
                columns.append(f'{name} {translate_type(field.annotation)}')

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
