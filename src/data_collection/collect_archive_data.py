import time
from collections import defaultdict
from datetime import datetime
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import NamedTuple, List, Dict, Optional

import requests

from configs.crawling import PREFIX, INTERNET_ARCHIVE_URL, INTERNET_ARCHIVE_TIMESTAMP_FORMAT
from configs.database import get_database_cursor, setup
from configs.utils import get_absolute_tranco_file_path
from data_collection.crawling import reset_failed_crawls, partition_jobs, crawl, CrawlingException

WORKERS = 8

TIMESTAMPS = [
    datetime(2016, 1, 15, 12), datetime(2016, 4, 15, 12), datetime(2016, 7, 15, 12), datetime(2016, 10, 15, 12),
    datetime(2017, 1, 15, 12), datetime(2017, 4, 15, 12), datetime(2017, 7, 15, 12), datetime(2017, 10, 15, 12),
    datetime(2018, 1, 15, 12), datetime(2018, 4, 15, 12), datetime(2018, 7, 15, 12), datetime(2018, 10, 15, 12),
    datetime(2019, 1, 15, 12), datetime(2019, 4, 15, 12), datetime(2019, 7, 15, 12), datetime(2019, 10, 15, 12),
    datetime(2020, 1, 15, 12), datetime(2020, 4, 15, 12), datetime(2020, 7, 15, 12), datetime(2020, 10, 15, 12),
    datetime(2021, 1, 15, 12), datetime(2021, 4, 15, 12), datetime(2021, 7, 15, 12), datetime(2021, 10, 15, 12),
    datetime(2022, 1, 15, 12), datetime(2022, 4, 15, 12), datetime(2022, 7, 15, 12), datetime(2022, 10, 15, 12),
    datetime(2023, 1, 15, 12), datetime(2023, 4, 15, 12),  # datetime(2023, 7, 15, 12), datetime(2023, 10, 15, 12),
]

TABLE_NAME = "archive_data_{timestamp}"


class ArchiveJob(NamedTuple):
    """Represents a job for crawling the archive and storing data in the database."""
    timestamp: str
    tranco_id: int
    domain: str
    url: str
    proxies: Optional[Dict[str, str]]


def worker(jobs: List[ArchiveJob]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        session = requests.Session()
        for timestamp, tranco_id, domain, url, proxies in jobs:
            time.sleep(0.2)
            try:
                response = crawl(url, proxies=proxies, session=session)
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME.format(timestamp=timestamp)} 
                    (tranco_id, domain, start_url, end_url, status_code, headers, content_hash, response_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, *response.serialized_data))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME.format(timestamp=timestamp)} 
                    (tranco_id, domain, start_url, headers) 
                    VALUES (%s, %s, %s, %s)
                """, (tranco_id, domain, url, error.to_json()))


def prepare_jobs(tranco_file: Path, timestamps: List[str], n: int = 20000) -> List[ArchiveJob]:
    """Generate ArchiveJob list for Tranco file, timestamps, and max domains per timestamp."""
    worked_urls = defaultdict(set)
    for timestamp in timestamps:
        worked_urls[timestamp] = reset_failed_crawls(TABLE_NAME.format(timestamp=timestamp))

    jobs = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            tranco_id, domain = line.strip().split(',')
            for timestamp in timestamps:
                url = INTERNET_ARCHIVE_URL.format(timestamp=timestamp, url=f"{PREFIX}{domain}")
                if url not in worked_urls[timestamp]:
                    jobs.append(ArchiveJob(timestamp, tranco_id, domain, url, None))

    return jobs


def run_jobs(jobs: List[ArchiveJob]) -> None:
    """Execute the provided crawl jobs using multiprocessing."""
    with Pool(WORKERS) as p:
        p.map(worker, partition_jobs(jobs, WORKERS))


def main():
    timestamps = [timestamp.strftime(INTERNET_ARCHIVE_TIMESTAMP_FORMAT) for timestamp in TIMESTAMPS]
    for timestamp in timestamps:
        setup(TABLE_NAME.format(timestamp=timestamp))

    # Prepare and execute the crawl jobs
    jobs = prepare_jobs(get_absolute_tranco_file_path(), timestamps)
    run_jobs(jobs)


if __name__ == '__main__':
    main()
