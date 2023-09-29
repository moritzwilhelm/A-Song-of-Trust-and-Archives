import json
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from psycopg2 import connect
from psycopg2.extensions import connection as connection_type, cursor as cursor_type
from psycopg2.extras import register_default_jsonb
from requests.structures import CaseInsensitiveDict

# DATABASE
DB_USER = '<DB_USER>'
DB_PWD = '<DB_PWD>'
DB_HOST = '<DB_HOST>'
DB_PORT = 1337
DB_NAME = '<DB_NAME>'

# STORAGE
STORAGE = Path('<PATH/TO/DATA/DIRECTORY/>')


def json_loads_ci(*args: Any, **kwargs: Any) -> Any:
    """Deserialize JSON data, transforming into a `CaseInsensitiveDict` if applicable."""
    deserialized_object = json.loads(*args, **kwargs)
    return CaseInsensitiveDict(deserialized_object) if isinstance(deserialized_object, dict) else deserialized_object


def get_database_connection(autocommit: bool = False) -> connection_type:
    """Establish a connection to the database and return the connection object."""
    connection = connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    connection.autocommit = autocommit
    register_default_jsonb(connection, loads=json_loads_ci)
    return connection


@contextmanager
def get_database_cursor(autocommit: bool = False) -> Generator[cursor_type, None, None]:
    """Establish a connection to the database and yield an open cursor."""
    connection = get_database_connection(autocommit)
    try:
        if autocommit:
            with connection.cursor() as cursor:
                yield cursor
        else:
            with connection, connection.cursor() as cursor:
                yield cursor
    finally:
        connection.close()


def get_min_timestamp(table_name: str) -> datetime:
    """Query the database for the `minimum timestamp` in `table_name`."""
    with get_database_cursor() as cursor:
        cursor.execute(f"SELECT MIN(timestamp) FROM {table_name}")
        return cursor.fetchone()[0]


def get_max_timestamp(table_name: str) -> datetime:
    """Query the database for the `maximum timestamp` in `table_name`."""
    with get_database_cursor() as cursor:
        cursor.execute(f"SELECT MAX(timestamp) FROM {table_name}")
        return cursor.fetchone()[0]
