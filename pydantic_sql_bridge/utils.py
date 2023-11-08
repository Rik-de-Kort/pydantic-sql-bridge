import sqlite3
from enum import Enum
from pydantic import BaseModel, Field
from typing import Type, TypeVar, Union, Any, Literal

from sqlglot import Dialects


class DatabaseType(Enum):
    SQLITE = Dialects.SQLITE
    MSSQL = Dialects.TSQL


Cursor = sqlite3.Cursor


def get_database_type(_c: Cursor) -> DatabaseType:
    return DatabaseType.SQLITE


def get_table_name(model_type: type) -> str:
    if hasattr(model_type, 'query_name'):
        return model_type.query_name
    else:
        return model_type.__name__


def get_model_name(table_name: str) -> str:
    return ''.join(word.capitalize() for word in table_name.split('_')) + 'Row'


def is_model(field: Field) -> bool:
    # field.annotation may be something like tuple[str, str] on which issubclass raises a type error
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


NOT_FOUND = object()  # sentinel


def lookup_underscored(source: dict, target_name: str) -> Union[Literal[NOT_FOUND], Any]:
    if target_name in source.keys():
        return source[target_name]
    matches = [name for name, field in source.items() if target_name.startswith(name)]
    results = [lookup_underscored(source[name], target_name.removeprefix(f'{name}_')) for name in matches]
    found = [r for r in results if r is not NOT_FOUND]
    if len(found) == 0: return NOT_FOUND
    if len(found) > 1: raise ValueError(f'Ambiguous name matches for {target_name} on {source}, got {results}.')
    return found[0]


T = TypeVar('T', bound=BaseModel)


def transform_dict(data: dict, target: Type[T]) -> dict:
    """
    Transform `data` to a shape matching with `target`'s attributes, where we map nested models using underscores.
    i.e. data['address']['street'] in a "nested" dict maps to data['address_street'] in a "flat" one and vice versa.
    """
    result = {}
    errors = []
    for target_field_name, target_field in target.model_fields.items():
        if target_field_name in data:
            result[target_field_name] = data[target_field_name]
        elif is_model(target_field):
            field_prefix = target_field.annotation.__name__.lower() + '_'
            field_source = {k.removeprefix(field_prefix): v for k, v in data.items() if k.startswith(field_prefix)}
            result[target_field_name] = transform_dict(field_source, target_field.annotation)
        else:
            result[target_field_name] = lookup_underscored(data, target_field_name)
            if result[target_field_name] == NOT_FOUND:
                errors.append(KeyError(f'Cannot find {target_field_name} in {data}.'))
    if len(errors) == 1:
        raise errors[0]
    elif errors:
        raise ExceptionGroup(f'Some names cannot be found', errors)
    return result


def dict_to_model(data: dict, model: Type[T]) -> T:
    errors = []
    # Convert nested models first
    for target_name, target_field in model.model_fields.items():
        if is_model(target_field):
            try:
                data[target_name] = dict_to_model(data[target_name], target_field.annotation)
            except Exception as e:
                errors.append(e)
    if len(errors) == 1:
        raise errors[0]
    elif errors:
        raise ExceptionGroup(f'Could not build model {model} from {data}', errors)
    return model(**data)


def transform(model: BaseModel, target: Type[T]) -> T:
    """
    Transform `model` to type `target`, where we map nested models using underscores.
    model.address.street in a "nested" model maps to model.address_street in a "flat" model and vice versa.
    """
    target_dict = transform_dict(model.model_dump(), target)
    return dict_to_model(target_dict, target)

