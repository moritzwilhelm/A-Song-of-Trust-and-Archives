from datetime import datetime

from analysis.header_utils import Headers
from configs.analysis import RELEVANT_HEADERS, INTERNET_ARCHIVE_HEADER_PREFIX, MEMENTO_HEADER
from configs.database import get_database_cursor


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


def parse_archived_headers(headers: Headers) -> Headers:
    """Only keep headers prefixed with 'X-Archive-Orig', strip the prefix, and attach the 'Memento-Datetime' header."""
    result = {
        header: headers[f"{INTERNET_ARCHIVE_HEADER_PREFIX}{header}"]
        for header in RELEVANT_HEADERS
        if f"{INTERNET_ARCHIVE_HEADER_PREFIX}{header}" in headers
    }
    # keep memento-datetime if present
    if MEMENTO_HEADER in headers:
        result[MEMENTO_HEADER] = headers[MEMENTO_HEADER]

    return Headers(result)
