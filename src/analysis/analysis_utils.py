from itertools import islice
from pathlib import Path
from typing import Any, List

from urllib3.util import parse_url

from configs.crawling import PREFIX
from configs.database import get_database_cursor


def parse_origin(url: str) -> str:
    """Extract the origin of a given URL."""
    parsed_url = parse_url(url)
    origin = f"{parsed_url.scheme}://{parsed_url.host}"
    if parsed_url.port is not None:
        origin += f":{parsed_url.port}"
    return origin


def get_tranco_urls(tranco_file: Path, n: int = 20000) -> List[str]:
    """Read `n` domains from the given `tranco_file` and expand them into full urls by prepending `PREFIX`."""
    urls = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            id_, domain = line.strip().split(',')
            urls.append(f"{PREFIX}{domain}")
    return urls


def get_aggregated_date(table_name: str, aggregation_function: str) -> Any:
    """Apply the `aggregation_function` on all dates in `table_name` and return the resulting value."""
    with get_database_cursor() as cursor:
        cursor.execute(f"SELECT {aggregation_function}(timestamp::date) FROM {table_name}")
        return cursor.fetchone()[0]
