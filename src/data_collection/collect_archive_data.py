from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from time import sleep
from typing import NamedTuple

import requests

from configs.crawling import NUMBER_URLS, INTERNET_ARCHIVE_URL, INTERNET_ARCHIVE_TIMESTAMP_FORMAT, TIMESTAMPS
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path, get_tranco_data
from data_collection.crawling import setup, reset_failed_archive_crawls, partition_jobs, CrawlingException, crawl

WORKERS = 8

TABLE_NAME = 'HISTORICAL_DATA'


class ArchiveJob(NamedTuple):
    """Represents a job for crawling the archive and storing data in the database."""
    timestamp: datetime
    tranco_id: int
    domain: str
    url: str
    proxies: dict[str, str] | None = None


def worker(jobs: list[ArchiveJob], table_name=TABLE_NAME) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        session = requests.Session()
        for timestamp, tranco_id, domain, url, proxies in jobs:
            sleep(0.2)
            try:
                response = crawl(url, proxies=proxies, session=session)
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    (tranco_id, domain, timestamp, start_url, end_url, status_code, headers, content_hash, response_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, domain, timestamp, url, *response.serialized_data))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    (tranco_id, domain, timestamp, start_url, headers)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tranco_id, domain, timestamp, url, error.to_json()))


def prepare_jobs(tranco_file: Path = get_absolute_tranco_file_path(),
                 timestamps: list[datetime] = TIMESTAMPS,
                 proxies: dict[str, str] | None = None,
                 n: int = NUMBER_URLS) -> list[ArchiveJob]:
    """Generate ArchiveJob list for Tranco file, timestamps, and max domains per timestamp."""
    worked_jobs = reset_failed_archive_crawls(TABLE_NAME)
    timestamp_strings = [timestamp.strftime(INTERNET_ARCHIVE_TIMESTAMP_FORMAT) for timestamp in timestamps]

    return [
        ArchiveJob(timestamp, tranco_id, domain, INTERNET_ARCHIVE_URL.format(timestamp=timestamp_str, url=url), proxies)
        for tranco_id, domain, url in get_tranco_data(tranco_file, n)
        for timestamp, timestamp_str in zip(timestamps, timestamp_strings)
        if tranco_id not in worked_jobs[timestamp]
    ]


def run_jobs(jobs: list[ArchiveJob]) -> None:
    """Execute the provided crawl jobs using multiprocessing."""
    with Pool(WORKERS) as pool:
        pool.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup(TABLE_NAME)

    # Prepare and execute the crawl jobs
    jobs = prepare_jobs()
    run_jobs(jobs)


if __name__ == '__main__':
    main()
