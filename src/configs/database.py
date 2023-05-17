# DATABASE
from contextlib import contextmanager

from psycopg2 import connect

DB_USER = 'archive'
DB_PWD = 'archive'
DB_HOST = '134.96.225.54'
DB_PORT = 5432
DB_NAME = 'archive_moritz'

# STORAGE
STORAGE = "/data/maws/"


@contextmanager
def get_database_cursor(autocommit: bool = False):
    """Establish a connection to the database and yield an open cursor."""
    with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = autocommit
        with connection.cursor() as cursor:
            yield cursor
