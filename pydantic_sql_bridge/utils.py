from contextlib import contextmanager
from enum import Enum
from pathlib import Path
import sqlite3
from typing import Any, Optional

Cursor = sqlite3.Cursor


class DatabaseType(Enum):
    SQLITE = 'SQLITE'


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


def query(c: Cursor, sql: str, data: Optional[tuple] = None) -> list[dict[str, Any]]:
    if data:
        result = c.execute(sql, data).fetchall()
    else:
        result = c.execute(sql).fetchall()
    columns = [name for name, *_ in c.description]
    return [dict(zip(columns, row)) for row in result]


def get_database_type(c: Cursor) -> DatabaseType:
    return DatabaseType.SQLITE
