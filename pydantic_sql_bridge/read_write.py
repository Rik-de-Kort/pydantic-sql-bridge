import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Type, Optional, Any, TypeVar

from pydantic import BaseModel
from sqlglot import transpile, Dialects
import sqlglot.expressions as exp

from pydantic_sql_bridge.utils import Cursor, get_database_type, get_table_name, is_model, get_primary_key


@contextmanager
def cursor(db_name: str | Path) -> Cursor:
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    try:
        yield c
    finally:
        try:
            c.close()
        finally:
            conn.close()


def raw_query(c: Cursor, sql: str, data: Optional[tuple] = None) -> list[dict[str, Any]]:
    if data:
        result = c.execute(sql, data).fetchall()
    else:
        result = c.execute(sql).fetchall()
    columns = [name for name, *_ in c.description]
    return [dict(zip(columns, row)) for row in result]


def build_query(model_type: Type[BaseModel]) -> exp.Select:
    table_name = get_table_name(model_type)
    result = exp.select().from_(table_name)
    for name, field in model_type.model_fields.items():
        if is_model(field):
            raise TypeError(f'Nested models not supported, field {name} is a Pydantic model.')
        result = result.select(f'{table_name}.{name}')
    return result


T = TypeVar('T', bound=BaseModel)


def get_where(c: Cursor, model_type: Type[T], **constraints) -> list[T]:
    """
    Retrieve Pydantic models from a database using cursor c, potentially matching constraints.
    Example:
    >>> get_where(c, User, id=24)
    [User(id=24, name='Jane Doe')]
    """
    if any(not_found := [col for col in constraints.keys() if col not in model_type.model_fields]):
        raise TypeError(f'columns {not_found} not found in model {model_type}')

    db_type = get_database_type(c)

    if not constraints:
        query = build_query(model_type)
        query_result = c.execute(query.sql(dialect=db_type.value)).fetchall()
    else:
        query = build_query(model_type)
        constraint_col_names, constraint_col_values = zip(*constraints.items())
        for name in constraint_col_names:
            query = query.where(f'{name} = ?', append=True)
        query_result = c.execute(query.sql(dialect=db_type.value), tuple(constraint_col_values)).fetchall()

    result_dicts = [{col.name: value for col, value in zip(query.selects, row)} for row in query_result]
    return [model_type(**d) for d in result_dicts]


def write(
        c: Cursor, models: list[BaseModel], compare_on: Optional[tuple[str, ...]] = None, *,
        should_insert=True, should_update=True, should_delete=False
):
    """
    Write models `models` to the database with an open cursor `c`. Determine updating, deleting, or inserting using
    the columns `compare_on`. If `compare_on` is not passed, check the models for a primary key annotation
    By default, this will insert and update, but not delete.
    """
    if len(models) == 0:
        raise TypeError(f'Cannot write empty list. If you want to delete, use delete_all.')
    if len(model_types := set(type(model) for model in models)) > 1:
        raise TypeError(f'Expected only one type of model in models, got {model_types}.')

    model_type = type(models[0])
    compare_on = get_primary_key(model_type) if compare_on is None else compare_on
    if not compare_on:
        raise TypeError(f'Cannot skip passing compare_on when model {model_type} has no primary key annotations')

    if any(not_present := [name for name in compare_on if name not in model_type.model_fields]):
        raise TypeError(f'Fields {not_present} in compare_on are not present in model {model_type}')

    write_dict_models(
        c, get_table_name(model_type), [model.model_dump() for model in models], compare_on,
        should_insert=should_insert, should_update=should_update, should_delete=should_delete
    )


def write_dict_models(
        c: Cursor, table_name: str, models: list[dict[str, Any]], compare_on: tuple[str], *,
        should_insert=True, should_update=True, should_delete=False
):
    database_type = get_database_type(c)
    get_id = lambda model: tuple(model[field] for field in compare_on)  # noqa

    sql_columns = ', '.join(compare_on)
    in_db = set(c.execute(f'select {sql_columns} from {table_name}').fetchall())
    in_memory = {get_id(model) for model in models}

    fields = tuple(models[0].keys())
    if (to_insert := in_memory - in_db) and should_insert:
        # We call them "columns" when they're in a sql string, "fields" otherwise
        sql_columns = ', '.join(fields)
        question_marks = ', '.join('?' for _ in fields)
        sql = f'INSERT INTO {table_name}({sql_columns}) VALUES ({question_marks})'

        insert_data = [
            tuple(model[field] for field in fields)
            for model in models if get_id(model) in to_insert
        ]
        c.executemany('\n'.join(transpile(sql, Dialects.SQLITE, database_type.value)), insert_data)

    if (to_update := in_memory & in_db) and should_update:
        cols_and_question_marks = ', '.join(f'{field}=?' for field in fields)
        keys_and_question_marks = ' AND '.join(f'{col}=?' for col in compare_on)
        sql = f'UPDATE {table_name} SET {cols_and_question_marks} WHERE {keys_and_question_marks}'

        update_data = [
            tuple(model[field] for field in fields) + tuple(model[field] for field in compare_on)
            for model in models if get_id(model) in to_update
        ]
        c.executemany('\n'.join(transpile(sql, Dialects.SQLITE, database_type.value)), update_data)

    if (to_delete := in_db - in_memory) and should_delete:
        cols_and_question_marks = 'AND '.join(f'{col}=?' for col in compare_on)
        sql = f'DELETE FROM {table_name} WHERE {cols_and_question_marks}'

        delete_data = [
            tuple(model[field] for field in compare_on)
            for model in models if get_id(model) in to_delete
        ]
        c.executemany('\n'.join(transpile(sql, Dialects.SQLITE, database_type.value)), delete_data)
