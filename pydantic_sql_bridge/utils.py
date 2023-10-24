import sqlite3
from enum import Enum
from pydantic import BaseModel, Field
from typing import Type

from sqlglot import Dialects


class DatabaseType(Enum):
    SQLITE = Dialects.SQLITE
    MSSQL = Dialects.TSQL


Cursor = sqlite3.Cursor


def get_database_type(c: Cursor) -> DatabaseType:
    return DatabaseType.SQLITE


def get_table_name(model_type: type) -> str:
    return model_type.__name__[:-3] if model_type.__name__.endswith('Row') else model_type.__name__


def get_model_name(table_name: str) -> str:
    return table_name.capitalize() + 'Row'


def is_model(field: Field) -> bool:
    # field.type_ may be something like tuple[str, str] on which issubclass raises a type error
    # typing.isclass returns True for this kind of thing though, so try-except it is
    try:
        return issubclass(field.annotation, BaseModel)
    except TypeError:
        return False


class Annotations(Enum):
    PRIMARY_KEY = 'PK'


def get_primary_key(model: Type[BaseModel]) -> tuple[str, ...]:
    pk_fields = []
    for name, field in model.model_fields.items():
        if any(datum == Annotations.PRIMARY_KEY for datum in field.metadata or []):
            pk_fields.append(name)
    return tuple(pk_fields)
