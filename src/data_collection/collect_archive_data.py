import time
from collections import defaultdict
from datetime import datetime
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import NamedTuple, List, Dict, Optional

import requests

from configs.crawling import PREFIX, INTERNET_ARCHIVE_URL
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path
from data_collection.crawling import setup, reset_failed_crawls, partition_jobs, crawl, CrawlingException

WORKERS = 8

DATES = [
    datetime(2016, 1, 15, 12), datetime(2016, 4, 15, 12), datetime(2016, 7, 15, 12), datetime(2016, 10, 15, 12),
    datetime(2017, 1, 15, 12), datetime(2017, 4, 15, 12), datetime(2017, 7, 15, 12), datetime(2017, 10, 15, 12),
    datetime(2018, 1, 15, 12), datetime(2018, 4, 15, 12), datetime(2018, 7, 15, 12), datetime(2018, 10, 15, 12),
    datetime(2019, 1, 15, 12), datetime(2019, 4, 15, 12), datetime(2019, 7, 15, 12), datetime(2019, 10, 15, 12),
    datetime(2020, 1, 15, 12), datetime(2020, 4, 15, 12), datetime(2020, 7, 15, 12), datetime(2020, 10, 15, 12),
    datetime(2021, 1, 15, 12), datetime(2021, 4, 15, 12), datetime(2021, 7, 15, 12), datetime(2021, 10, 15, 12),
    datetime(2022, 1, 15, 12), datetime(2022, 4, 15, 12), datetime(2022, 7, 15, 12), datetime(2022, 10, 15, 12),
    datetime(2023, 1, 15, 12), datetime(2023, 4, 15, 12),  # datetime(2023, 7, 15, 12), datetime(2023, 10, 15, 12),
]

TABLE_NAME = "archive_data_{date}"


class ArchiveJob(NamedTuple):
    """Represents a job for crawling the archive and storing data in the database."""
    date: str
    tranco_id: int
    domain: str
    url: str
    proxies: Optional[Dict[str, str]]


def worker(jobs: List[ArchiveJob]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        session = requests.Session()
        for date, tranco_id, domain, url, proxies in jobs:
            time.sleep(0.2)
            try:
                response = crawl(url, proxies=proxies, session=session)
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME.format(date=date)} 
                    (tranco_id, domain, start_url, end_url, status_code, headers, content_hash, response_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, *response.serialized_data))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME.format(date=date)} 
                    (tranco_id, domain, start_url, headers) 
                    VALUES (%s, %s, %s, %s)
                """, (tranco_id, domain, url, error.to_json()))


def prepare_jobs(tranco_file: Path, dates: List[str], n: int = 20000) -> List[ArchiveJob]:
    """Build a list of ArchiveJob instances for the given Tranco file, dates, and maximum number of domains per date."""
    worked_urls = defaultdict(set)
    for date in dates:
        worked_urls[date] = reset_failed_crawls(TABLE_NAME.format(date=date))

    jobs = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            tranco_id, domain = line.strip().split(',')
            for date in dates:
                url = INTERNET_ARCHIVE_URL.format(date=date, url=f"{PREFIX}{domain}")
                if url not in worked_urls[date]:
                    jobs.append(ArchiveJob(date, tranco_id, domain, url, None))

    return jobs


def run_jobs(jobs: List[ArchiveJob]) -> None:
    """Execute the provided crawl jobs using multiprocessing."""
    with Pool(WORKERS) as p:
        p.map(worker, partition_jobs(jobs, WORKERS))


def main():
    dates = [date.strftime('%Y%m%d%H%M%S') for date in DATES]
    for date in dates:
        setup(TABLE_NAME.format(date=date))

    # Prepare and execute the crawl jobs
    jobs = prepare_jobs(get_absolute_tranco_file_path(), dates)
    run_jobs(jobs)


if __name__ == '__main__':
    main()
