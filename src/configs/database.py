import json
from contextlib import contextmanager
from typing import Any

from psycopg2 import connect
from psycopg2._json import register_default_jsonb
from requests.structures import CaseInsensitiveDict

# DATABASE
DB_USER = 'archive'
DB_PWD = 'archive'
DB_HOST = '134.96.225.54'
DB_PORT = 5432
DB_NAME = 'archive_moritz'

# STORAGE
STORAGE = "/data/maws/"


def json_loads_ci(*args: Any, **kwargs: Any) -> Any:
    """Deserialize JSON data, transforming into a `CaseInsensitiveDict` if applicable."""
    deserialized_object = json.loads(*args, **kwargs)
    return CaseInsensitiveDict(deserialized_object) if isinstance(deserialized_object, dict) else deserialized_object


@contextmanager
def get_database_cursor(autocommit: bool = False):
    """Establish a connection to the database and yield an open cursor."""
    connection = connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD)
    connection.autocommit = autocommit
    register_default_jsonb(connection, loads=json_loads_ci)
    try:
        if autocommit:
            with connection.cursor() as cursor:
                yield cursor
        else:
            with connection, connection.cursor() as cursor:
                yield cursor
    finally:
        connection.close()


def setup(table_name: str) -> None:
    """Create crawling database table and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                tranco_id INTEGER,
                domain VARCHAR(128),
                start_url VARCHAR(128),
                end_url TEXT DEFAULT NULL,
                status_code INT DEFAULT NULL,
                headers JSONB DEFAULT NULL,
                content_hash VARCHAR(64) DEFAULT NULL,
                response_time NUMERIC DEFAULT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        for column in ['tranco_id', 'domain', 'start_url', 'end_url', 'status_code', 'content_hash', 'response_time',
                       'timestamp']:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_{column}_idx ON {table_name} ({column})")
