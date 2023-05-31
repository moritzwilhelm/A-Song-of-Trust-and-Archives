import time
from collections import defaultdict
from datetime import datetime
from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import List, Tuple

import requests

from configs.crawling import PREFIX, INTERNET_ARCHIVE_URL
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path
from data_collection.crawling import setup, reset_failed_crawls, crawl, partition_jobs

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
PROXIES = None


def worker(urls: List[Tuple[str, int, str, str]]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        session = requests.Session()
        for date, tranco_id, domain, url in urls:
            time.sleep(0.2)
            success, data = crawl(url, proxies=PROXIES, session=session)
            if success:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME.format(date=date)} 
                    (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, *data))
            else:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME.format(date=date)} 
                    (tranco_id, domain, start_url, headers) 
                    VALUES (%s, %s, %s, to_json(%s::text)::jsonb)
                """, (tranco_id, domain, url, data))


def collect_data(tranco_file: Path, dates: List[str], n: int = 20000) -> None:
    """Crawl `n` domains in the `tranco_file` for each date in `dates`."""
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
                    jobs.append((date, tranco_id, domain, url))

    with Pool(WORKERS) as p:
        p.map(worker, partition_jobs(jobs, WORKERS))


def main(dates=None, proxies=PROXIES):
    if dates is None:
        dates = DATES

    global PROXIES
    PROXIES = proxies

    dates = [datetime.strftime(date, '%Y%m%d%H%M%S') for date in dates]
    for date in dates:
        setup(TABLE_NAME.format(date=date))

    collect_data(get_absolute_tranco_file_path(), dates)


if __name__ == '__main__':
    main()
