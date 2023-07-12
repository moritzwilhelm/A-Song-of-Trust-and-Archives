from typing import Any, Tuple

import pandas as pd
from urllib3.util import parse_url

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


def get_aggregated_date(table_name: str, aggregation_function: str) -> Any:
    """Apply the `aggregation_function` on all dates in `table_name` and return the resulting value."""
    with get_database_cursor() as cursor:
        cursor.execute(f"SELECT {aggregation_function}(timestamp::date) FROM {table_name}")
        return cursor.fetchone()[0]
