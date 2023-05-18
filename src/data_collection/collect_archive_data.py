import time
from itertools import islice
from multiprocessing import Pool
from pathlib import Path

import requests

from configs.crawling import PREFIX, INTERNET_ARCHIVE_URL
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path
from data_collection.crawling import setup, reset_failed_crawls, crawl

WORKERS = 8

DATE = '20230501'
TABLE_NAME = f"archive_data_{DATE}"
IA_URL_PREFIX_LENGTH = len(INTERNET_ARCHIVE_URL.format(date=DATE, url=f"{PREFIX}"))


def worker(urls: list[str]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        session = requests.Session()
        for tranco_id, url in urls:
            time.sleep(0.2)
            success, data = crawl(url, session=session)
            if success:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} 
                    (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, url[IA_URL_PREFIX_LENGTH:], url, *data))
            else:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} 
                    (tranco_id, domain, start_url, headers) 
                    VALUES (%s, %s, %s, to_json(%s::text)::jsonb)
                """, (tranco_id, url[IA_URL_PREFIX_LENGTH:], url, data))


def collect_data(tranco_file: Path, n: int = 20000) -> None:
    """Crawl `n` domains in the `tranco_file`."""
    worked_urls = reset_failed_crawls(TABLE_NAME)

    urls = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            tranco_id, domain = line.strip().split(',')
            url = INTERNET_ARCHIVE_URL.format(date=DATE, url=f"{PREFIX}{domain}")
            if url not in worked_urls:
                urls.append((tranco_id, url))

    chunks = [urls[i:i + len(urls) // WORKERS] for i in range(0, len(urls), len(urls) // WORKERS)]

    with Pool(WORKERS) as p:
        p.map(worker, chunks)


def main():
    setup(TABLE_NAME)
    collect_data(get_absolute_tranco_file_path())


if __name__ == '__main__':
    main()
