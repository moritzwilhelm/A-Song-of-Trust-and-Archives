from collections import defaultdict
from datetime import datetime, timedelta
from itertools import chain
from multiprocessing import Pool
from pathlib import Path
from time import sleep
from typing import NamedTuple, List, Optional, Dict

import requests
from psycopg2.extras import Json
from pytz import utc
from requests import Session, JSONDecodeError

from configs.crawling import NUMBER_URLS, INTERNET_ARCHIVE_TIMESTAMP_FORMAT, TIMESTAMPS, INTERNET_ARCHIVE_URL
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path, get_tranco_data
from data_collection.collect_archive_data import WORKERS, ArchiveJob, worker as archive_worker
from data_collection.crawling import setup, reset_failed_archive_crawls, partition_jobs, CrawlingException, crawl

CANDIDATES_WORKERS = 2

TABLE_NAME = 'HISTORICAL_DATA_PROXIMITY_SETS'
CANDIDATES_TABLE_NAME = 'PROXIMITY_SET_CANDIDATES'

CDX_REQUEST = 'https://web.archive.org/cdx/search/cdx' + \
              '?url={url}&output=json&fl=timestamp&filter=!statuscode:3..&from={from_timestamp}&to={to_timestamp}'


class CdxJob(NamedTuple):
    """Represents a job for crawling the Internet Archive CDX server and storing data in the database."""
    timestamp: datetime
    tranco_id: int
    domain: str
    url: str
    proxies: Optional[Dict[str, str]] = None


def setup_candidates_lists_table() -> None:
    """Create proximity set candidates database table and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {CANDIDATES_TABLE_NAME} (
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
                CREATE INDEX IF NOT EXISTS {CANDIDATES_TABLE_NAME}_{column}_idx ON {CANDIDATES_TABLE_NAME} ({column})
            """)


def reset_failed_cdx_crawls() -> Dict[datetime, List[int]]:
    """Delete all crawling results with an error."""
    with get_database_cursor(autocommit=True) as cursor:
        cursor.execute(f"DELETE FROM {CANDIDATES_TABLE_NAME} WHERE error IS NOT NULL")
        cursor.execute(f"SELECT timestamp, ARRAY_AGG(tranco_id) FROM {CANDIDATES_TABLE_NAME} GROUP BY timestamp")
        return defaultdict(list, cursor.fetchall())


def find_candidates(url: str,
                    timestamp: datetime,
                    n: int,
                    proxies: Optional[Dict[str, str]] = None,
                    session: Optional[Session] = None) -> List[str]:
    """Collect the `n` best candidates for the proximity set of (`url`, `timestamp`)."""
    response = crawl(
        CDX_REQUEST.format(
            url=url,
            from_timestamp=(timestamp - timedelta(days=3, hours=12)).strftime(INTERNET_ARCHIVE_TIMESTAMP_FORMAT),
            to_timestamp=(timestamp + timedelta(days=3, hours=12)).strftime(INTERNET_ARCHIVE_TIMESTAMP_FORMAT)
        ),
        proxies=proxies,
        session=session
    )

    def timestamp_distance(value: str) -> timedelta:
        return abs(timestamp - datetime.strptime(value, INTERNET_ARCHIVE_TIMESTAMP_FORMAT).replace(tzinfo=utc))

    try:
        timestamps = response.json()
    except JSONDecodeError as error:
        raise CrawlingException(url) from error

    if timestamps:
        timestamps = sorted(set(chain.from_iterable(timestamps[1:])), key=timestamp_distance)

    return timestamps[:n]


def cdx_worker(jobs: List[CdxJob]) -> None:
    """Crawl the CDX server for all provided `urls` and `timestamps` and store the responses in the database."""
    session = requests.Session()
    with get_database_cursor(autocommit=True) as cursor:
        for timestamp, tranco_id, domain, url, proxies in jobs:
            sleep(0.2)
            try:
                candidates = find_candidates(url, timestamp, 25, proxies, session)
                cursor.execute(f"""
                    INSERT INTO {CANDIDATES_TABLE_NAME} (tranco_id, domain, url, timestamp, candidates)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, timestamp, Json(candidates)))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {CANDIDATES_TABLE_NAME} (tranco_id, domain, url, timestamp, error)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, timestamp, error.to_json()))


def crawl_web_archive_cdx(tranco_file: Path = get_absolute_tranco_file_path(),
                          timestamps: List[datetime] = TIMESTAMPS,
                          n: int = NUMBER_URLS,
                          proxies: Optional[Dict[str, str]] = None) -> None:
    """Crawl the Internet Archive CDX server for candidates for each (url, timestamp) proximity set."""
    worked_jobs = reset_failed_cdx_crawls()

    jobs = [
        CdxJob(timestamp, tranco_id, domain, url, proxies)
        for tranco_id, domain, url in get_tranco_data(tranco_file, n)
        for timestamp in timestamps
        if tranco_id not in worked_jobs[timestamp]
    ]

    with Pool(CANDIDATES_WORKERS) as pool:
        pool.map(cdx_worker, partition_jobs(jobs, CANDIDATES_WORKERS))


def proximity_sets_worker(jobs: List[ArchiveJob]) -> None:
    """Wrapper function for the Internet Archive worker that fixes the table name to `TABLE_NAME`."""
    return archive_worker(jobs, table_name=TABLE_NAME)


def crawl_proximity_sets(timestamps: List[datetime] = TIMESTAMPS,
                         proxies: Optional[Dict[str, str]] = None,
                         n: int = 10) -> None:
    """Crawl the Internet Archive for the `n` closest candidates of the proximity set."""
    worked_jobs = reset_failed_archive_crawls(TABLE_NAME)
    jobs = []
    with get_database_cursor() as cursor:
        for timestamp in timestamps:
            cursor.execute(f"""
                SELECT tranco_id, domain, url, candidates
                FROM {CANDIDATES_TABLE_NAME}
                WHERE timestamp=%s AND candidates!='[]'
            """, (timestamp,))

            jobs += [
                ArchiveJob(ts, tid, domain, INTERNET_ARCHIVE_URL.format(timestamp=timestamp_str, url=url), proxies)
                for tid, domain, url, candidates in cursor.fetchall()
                for timestamp_str in candidates[:n]
                for ts in (datetime.strptime(timestamp_str, INTERNET_ARCHIVE_TIMESTAMP_FORMAT).replace(tzinfo=utc),)
                if tid not in worked_jobs[ts]
            ]

    with Pool(WORKERS) as pool:
        pool.map(proximity_sets_worker, partition_jobs(jobs, WORKERS))


def main():
    setup_candidates_lists_table()
    crawl_web_archive_cdx()

    setup(TABLE_NAME)
    crawl_proximity_sets()


if __name__ == '__main__':
    main()
