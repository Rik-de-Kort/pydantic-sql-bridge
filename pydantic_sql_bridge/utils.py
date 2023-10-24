import sqlite3
from enum import Enum
from pydantic import BaseModel, Field
from typing import Any

from sqlglot import Dialects


class DatabaseType(Enum):
    SQLITE = Dialects.SQLITE
    MSSQL = Dialects.TSQL


Cursor = sqlite3.Cursor


def get_database_type(c: Cursor) -> DatabaseType:
    return DatabaseType.SQLITE


def content_hash(thing: Any) -> int:
    try:
        return hash(thing)
    except TypeError:
        pass
    if isinstance(thing, Mapping):
        # Mapping means not just dicts, keys might not be hashable
        return hash(tuple((content_hash(k), content_hash(v)) for k, v in thing.items()))
    elif isinstance(thing, Iterable):
        return hash(tuple(content_hash(item) for item in thing))
    elif isinstance(thing, object):
        return hash(tuple((k, content_hash(v)) for k, v in thing.__dict__ if not k.startswith('__')))
    else:
        raise ValueError(f'Tremendously unhashable type {type(thing)}, value {thing}')


def model_hash(model: BaseModel) -> int:
    return content_hash(tuple((field, getattr(model, field)) for field in model.__fields__.keys()))


def get_table_name(model_type: type) -> str:
    return model_type.__name__[-3] if model_type.__name__.endswith('Row') else model_type.__name__


def get_model_name(table_name: str) -> str:
    return table_name.capitalize() + 'Row'


def is_model(field: Field) -> bool:
    # field.type_ may be something like tuple[str, str] on which issubclass raises a type error
    # typing.isclass returns True for this kind of thing though, so try-except it is
    try:
        return issubclass(field.annotation, BaseModel)
    except TypeError:
        return False
