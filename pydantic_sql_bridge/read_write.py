import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Type, Optional, Any

from pydantic import BaseModel
from sqlglot import transpile, Dialects, parse_one
import sqlglot.expressions as exp

from pydantic_sql_bridge.utils import Cursor, get_database_type, get_table_name, is_model


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
    for name, field in model_type.__fields__.items():
        if not is_model(field):
            result = result.select(f'{table_name}.{name}')
        else:
            sub_query = build_query(field.type_)
            sub_table = sub_query.args['from'].expressions[0]
            join_expr = exp.Join(this=sub_table, kind='inner')
            for col in field.type_.__id__:
                join_expr = join_expr.on(f'{table_name}.{sub_table}_{col} = {sub_table}.{col}', append=True)
            result = result.select(*sub_query.selects).join(join_expr, append=True)
            for join_expr in sub_query.args.get('joins', []):
                result = result.join(join_expr, append=True)
    return result


def build_model(grouped_selects: dict[str, dict[str, Any]], model_type: Type[BaseModel]) -> BaseModel:
    model_name = get_table_name(model_type)
    as_dict = {}
    for name, field in model_type.__fields__.items():
        if not is_model(field):
            as_dict[name] = grouped_selects[model_name][name]
        else:
            as_dict[name] = build_model(grouped_selects, field.type_)
    return model_type(**as_dict)


def get_where(c: Cursor, model_type: Type[BaseModel], **constraints) -> list[BaseModel]:
    """
    Retrieve Pydantic models from a database using cursor c, potentially matching constraints.
    Example:
    >>> get_where(c, User, id=24)
    [User(id=24, name='Jane Doe')]
    """
    if any(not_found := [col for col in constraints.keys() if col not in model_type.__fields__]):
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

    result = []
    for row in query_result:
        # We don't push this selection grouping into build_model because it complicates the contract that the function
        # assumes: in that case, all the fields would have to be aligned in terms of position, and we'd have to pass in
        # the query. This is a bit cleaner.
        grouped_selects = defaultdict(lambda: {})
        for col, value in zip(query.selects, row):
            grouped_selects[col.table][col.name] = value
        result.append(build_model(grouped_selects, model_type))
    return result


def flatten_model(model: BaseModel) -> dict:
    """Flatten a model into a dictionary, with the primary keys of all referring models pulled out."""
    result = {}
    for name, field in model.__fields__.items():
        if not is_model(field):
            result[name] = getattr(model, name)
        else:
            submodel = getattr(model, name)
            if not hasattr(submodel, '__id__'):
                raise TypeError(f'Cannot flatten {model} because {submodel} does not have attribute __id__')
            table_name = get_table_name(type(submodel))
            for subname in submodel.__id__:
                result[f'{table_name}_{subname}'] = getattr(submodel, subname)
    return result


def write(
        c: Cursor, models: list[BaseModel], compare_on: Optional[tuple[str]] = None, *,
        should_insert=True, should_update=True, should_delete=False
):
    """
    Write models `models` to the database with an open cursor `c`. Determine updating, deleting, or inserting using
    the columns `compare_on`. If `compare_on` is not passed, we use the __id__ attribute on the models.
    By default, this will insert and update, but not delete.
    """
    if len(models) == 0:
        raise TypeError(f'Cannot write empty list. If you want to delete, use delete_all.')
    if len(model_types := set(type(model) for model in models)) > 1:
        raise TypeError(f'Expected only one type of model in models, got {model_types}.')

    model_type = type(models[0])
    if compare_on is None and not hasattr(model_type, '__id__'):
        raise TypeError(f'Cannot skip passing compare_on when model {model_type} has no attribute __id__')
    elif compare_on is None:
        compare_on = model_type.__id__

    if any(not_present := [name for name in compare_on if name not in model_type.__fields__]):
        raise TypeError(f'Fields {not_present} in compare_on are not present in model {model_type}')

    write_dict_models(
        c, get_table_name(model_type), [flatten_model(model) for model in models], compare_on,
        should_insert=should_insert, should_update=should_update, should_delete=should_delete
    )


def write_dict_models(
        c: Cursor, table_name: str, models: list[dict[str, Any]], compare_on: tuple[str], *,
        should_insert=True, should_update=True, should_delete=False
):
    database_type = get_database_type(c)
    get_id = lambda model: tuple(model[field] for field in compare_on)

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
