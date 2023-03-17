import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Type, Optional, Any

from pydantic import BaseModel

from pydantic_sql_bridge.pydantic_first import get_table_name
from pydantic_sql_bridge.utils import Cursor


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


def get_where(c: Cursor, model_type: Type[BaseModel], **constraints) -> list[BaseModel]:
    """
    Retrieve Pydantic models from a database using cursor c, potentially matching constraints.
    Example:
    >>> get_where(c, User, id=24)
    [User(id=24, name='Jane Doe')]
    """
    if any(not_found := [col for col in constraints.keys() if col not in model_type.__fields__]):
        raise TypeError(f'columns {not_found} not found in model {model_type}')

    table_name = get_table_name(model_type)

    if constraints:
        constraint_col_names, constraint_col_values = zip(*constraints.items())
        constraints_sql = ' AND '.join(f'{col}=?' for col in constraint_col_names)
        query_result = raw_query(c, f'SELECT * FROM {table_name} WHERE {constraints_sql}', tuple(constraint_col_values))
    else:
        query_result = raw_query(c, f'SELECT * FROM {table_name}')
    return [model_type(**row) for row in query_result]


# Todo: make polyglot
def write(
        c: Cursor, models: list[BaseModel], compare_on: Optional[tuple[str]] = None, *,
        should_insert=True, should_update=True, should_delete=False
):
    if len(models) == 0:
        raise TypeError(f'Cannot write empty list. If you want to delete, use delete_all.')
    if len(model_types := set(type(model) for model in models)) > 1:
        raise TypeError(f'Expected only one type of model in models, got {model_types}.')

    model_type = type(models[0])
    if any(fk := [field for field in model_type.__fields__.values() if issubclass(field.type_, BaseModel)]):
        raise TypeError(f'Foreign keys not yet supported, found {fk} on {model_type}')
    if compare_on is None and not hasattr(model_type, '__id__'):
        raise TypeError(f'Cannot skip passing compare_on when model {model_type} has no attribute __id__')
    elif compare_on is None:
        compare_on = model_type.__id__

    if any(not_present := [name for name in compare_on if name not in model_type.__fields__]):
        raise TypeError(f'Fields {not_present} in compare_on are not present in model {model_type}')

    table_name = get_table_name(model_type)
    sql_columns = ', '.join(compare_on)
    in_db = set(c.execute(f'select {sql_columns} from {table_name}').fetchall())
    in_memory = {tuple(getattr(model, field) for field in compare_on) for model in models}
    get_id = lambda model: tuple(getattr(model, field) for field in compare_on)

    fields = tuple(model_type.__fields__.keys())
    if (to_insert := in_memory - in_db) and should_insert:
        # We call them "columns" when they're in a sql string, "fields" otherwise
        sql_columns = ', '.join(fields)
        question_marks = ', '.join('?' for _ in fields)
        sql = f'INSERT INTO {table_name}({sql_columns}) VALUES ({question_marks})'

        insert_data = [
            tuple(getattr(model, field) for field in fields)  # Todo: there must be a better way!
            for model in models if get_id(model) in to_insert
        ]
        c.executemany(sql, insert_data)

    if (to_update := in_memory & in_db) and should_update:
        cols_and_question_marks = ', '.join(f'{field}=?' for field in fields)
        keys_and_question_marks = ' AND '.join(f'{col}=?' for col in compare_on)
        sql = f'UPDATE {table_name} SET {cols_and_question_marks} WHERE {keys_and_question_marks}'

        update_data = [
            tuple(getattr(model, field) for field in fields) + tuple(getattr(model, field) for field in compare_on)
            for model in models if get_id(model) in to_update
        ]
        c.executemany(sql, update_data)

    if (to_delete := in_db - in_memory) and should_delete:
        cols_and_question_marks = 'AND '.join(f'{col}=?' for col in compare_on)
        sql = f'DELETE FROM {table_name} WHERE {cols_and_question_marks}'

        delete_data = [
            tuple(getattr(model, field) for field in compare_on)
            for model in models if get_id(model) in to_delete
        ]
        c.executemany(sql, delete_data)
