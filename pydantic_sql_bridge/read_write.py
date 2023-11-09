from typing import Type, Optional, Any, TypeVar

from pggm_datalab_utils import db
from pydantic import BaseModel
import sqlglot.expressions as exp

from pydantic_sql_bridge.utils import (
    Cursor,
    get_database_type,
    get_table_name,
    is_model,
    get_primary_key,
)

cursor = db.cursor


def raw_query(
        c: Cursor, sql: str, data: Optional[tuple] = None
) -> list[dict[str, Any]]:
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
            raise TypeError(
                f"Nested models not supported, field {name} is a Pydantic model."
            )
        result = result.select(f"{table_name}.{name}")
    return result


T = TypeVar("T", bound=BaseModel)


def get_where(c: Cursor, model_type: Type[T], **constraints) -> list[T]:
    """
    Retrieve Pydantic models from a database using cursor c, potentially matching constraints.
    Example:
    >>> get_where(c, User, id=24)
    [User(id=24, name='Jane Doe')]
    """
    if any(
            not_found := [
                col for col in constraints.keys() if col not in model_type.model_fields
            ]
    ):
        raise TypeError(f"columns {not_found} not found in model {model_type}")

    db_type = get_database_type(c)

    if not constraints:
        query = build_query(model_type)
        query_result = c.execute(query.sql(dialect=db_type.value)).fetchall()
    else:
        query = build_query(model_type)
        constraint_col_names, constraint_col_values = zip(*constraints.items())
        for name in constraint_col_names:
            query = query.where(f"{name} = ?", append=True)
        query_result = c.execute(
            query.sql(dialect=db_type.value), tuple(constraint_col_values)
        ).fetchall()

    result_dicts = [
        {col.name: value for col, value in zip(query.selects, row)}
        for row in query_result
    ]
    return [model_type(**d) for d in result_dicts]


def write(
        c: Cursor,
        models: list[BaseModel],
        compare_on: Optional[tuple[str, ...]] = None,
        *,
        should_insert=True,
        should_update=True,
        should_delete=False,
):
    """
    Write models `models` to the database with an open cursor `c`. Determine updating, deleting, or inserting using
    the columns `compare_on`. If `compare_on` is not passed, check the models for a primary key annotation
    By default, this will insert and update, but not delete.
    """
    if len(models) == 0:
        raise TypeError(
            f"Cannot write empty list. If you want to delete, use delete_all."
        )
    if len(model_types := set(type(model) for model in models)) > 1:
        raise TypeError(
            f"Expected only one type of model in models, got {model_types}."
        )

    model_type = type(models[0])
    compare_on = get_primary_key(model_type) if compare_on is None else compare_on
    if not compare_on:
        raise TypeError(
            f"Cannot skip passing compare_on when model {model_type} has no primary key annotations"
        )

    if any(
            not_present := [
                name for name in compare_on if name not in model_type.model_fields
            ]
    ):
        raise TypeError(
            f"Fields {not_present} in compare_on are not present in model {model_type}"
        )

    db.write(
        c,
        get_table_name(model_type),
        [model.model_dump() for model in models],
        compare_on,
        insert=should_insert,
        update=should_update,
        delete=should_delete,
    )
