from typing import Any, Tuple

import pandas as pd
from urllib3.util import parse_url

from analysis.header_utils import Headers
from configs.analysis import RELEVANT_HEADERS, INTERNET_ARCHIVE_HEADER_PREFIX, MEMENTO_HEADER
from configs.database import get_database_cursor, get_database_connection


def sql_to_dataframe(query: str, query_args: Tuple[Any, ...] = ()) -> pd.DataFrame:
    """Read SQL query into a Pandas DataFrame."""
    with get_database_cursor() as cursor:
        query = cursor.mogrify(query, query_args)
    return pd.read_sql(query.decode(), con=get_database_connection())


def parse_origin(url: str) -> str:
    """Extract the origin of a given URL."""
    parsed_url = parse_url(url)
    origin = f"{parsed_url.scheme}://{parsed_url.host}"
    if parsed_url.port is not None:
        origin += f":{parsed_url.port}"
    return origin


def get_aggregated_timestamp(table_name: str, aggregation_function: str) -> Any:
    """Apply the `aggregation_function` on all timestamps in `table_name` and return the resulting value."""
    with get_database_cursor() as cursor:
        cursor.execute(f"SELECT {aggregation_function}(timestamp) FROM {table_name}")
        return cursor.fetchone()[0]

def get_aggregated_timestamp_date(table_name: str, aggregation_function: str) -> Any:
    """Apply the `aggregation_function` on all timestamp::dates in `table_name` and return the resulting value."""
    with get_database_cursor() as cursor:
        cursor.execute(f"SELECT {aggregation_function}(timestamp::date) FROM {table_name}")
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
