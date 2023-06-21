from datetime import datetime, timedelta
from itertools import chain
from multiprocessing import Pool
from pathlib import Path
from typing import NamedTuple, List

import requests
from psycopg2.extras import Json
from pytz import utc
from requests import JSONDecodeError

from configs.crawling import INTERNET_ARCHIVE_TIMESTAMP_FORMAT
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path, get_tranco_data
from data_collection.collect_archive_data import TIMESTAMPS
from data_collection.crawling import crawl, partition_jobs, CrawlingException

WORKERS = 8

CDX_REQUEST = "https://web.archive.org/cdx/search/cdx" + \
              "?url={url}&output=json&fl=timestamp&filter=!statuscode:3..&from={from_timestamp}&to={to_timestamp}"


class CdxJob(NamedTuple):
    """Represents a job for crawling the Internet Archive CDX server and storing data in the database."""
    timestamp: str
    tranco_id: int
    domain: str
    url: str


def setup_candidate_lists_table() -> None:
    """Create proximity set candidates database table and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS PROXIMITY_SET_CANDIDATES (
                id SERIAL PRIMARY KEY,
                tranco_id INTEGER,
                domain VARCHAR(128),
                url VARCHAR(128),
                timestamp TIMESTAMPTZ DEFAULT NULL,
                candidates JSONB DEFAULT NULL,
                error JSONB DEFAULT NULL
            );
        """)

        for column in ['tranco_id', 'domain', 'url', 'timestamp', 'candidates', 'error']:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS PROXIMITY_SET_CANDIDATES_{column}_idx ON PROXIMITY_SET_CANDIDATES ({column})
            """)


def find_candidates(url: str, timestamp: datetime, n: int, session=None) -> List[str]:
    """Collect the `n` best candidates for the proximity set of (`url`, `timestamp`)."""
    response = crawl(
        CDX_REQUEST.format(
            url=url,
            from_timestamp=(timestamp - timedelta(days=3, hours=12)).strftime(INTERNET_ARCHIVE_TIMESTAMP_FORMAT),
            to_timestamp=(timestamp + timedelta(days=3, hours=12)).strftime(INTERNET_ARCHIVE_TIMESTAMP_FORMAT)
        ),
        session=session
    )

    def timestamp_distance(value: str) -> timedelta:
        return abs(timestamp - datetime.strptime(value, INTERNET_ARCHIVE_TIMESTAMP_FORMAT).replace(tzinfo=utc))

    try:
        timestamps = response.json()
    except JSONDecodeError as error:
        raise CrawlingException(url) from error

    if timestamps:
        timestamps = sorted(chain.from_iterable(timestamps[1:]), key=timestamp_distance)

    return timestamps[:n]


def worker(jobs: List[CdxJob]) -> None:
    """Crawl the CDX server for all provided `urls` and `timestamps` and store the responses in the database."""
    session = requests.Session()
    with get_database_cursor(autocommit=True) as cursor:
        for timestamp, tranco_id, domain, url in jobs:
            try:
                candidates = find_candidates(url, timestamp, 25, session)
                cursor.execute("""
                    INSERT INTO PROXIMITY_SET_CANDIDATES (tranco_id, domain, url, timestamp, candidates)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, timestamp, Json(candidates)))
            except CrawlingException as error:
                cursor.execute("""
                    INSERT INTO PROXIMITY_SET_CANDIDATES (tranco_id, domain, url, timestamp, error)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, timestamp, error.to_json()))


def crawl_web_archive_cdx(tranco_file: Path, n: int = 20000) -> None:
    """Crawl the Internet Archive CDX server for candidates for each (url, timestamp) proximity set."""
    jobs = [CdxJob(ts, tid, domain, url) for tid, domain, url in get_tranco_data(tranco_file, n) for ts in TIMESTAMPS]

    with Pool(WORKERS) as pool:
        pool.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup_candidate_lists_table()
    crawl_web_archive_cdx(get_absolute_tranco_file_path())


if __name__ == '__main__':
    main()
