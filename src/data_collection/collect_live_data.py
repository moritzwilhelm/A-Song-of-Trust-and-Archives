from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import List, Tuple

from configs.crawling import PREFIX
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path
from data_collection.crawling import setup, reset_failed_crawls, crawl, partition_jobs

WORKERS = 8

TABLE_NAME = 'live_data'


def worker(urls: List[Tuple[int, str, str]]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        for tranco_id, domain, url in urls:
            success, data = crawl(url)
            if success:
                cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (tranco_id, domain, url, *data))
            else:
                cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, headers) 
                        VALUES (%s, %s, %s, to_json(%s::text)::jsonb)
                    """, (tranco_id, domain, url, data))


def collect_data(tranco_file: Path, n: int = 20000) -> None:
    """Crawl `n` domains in the `tranco_file`."""
    worked_urls = reset_failed_crawls(TABLE_NAME)

    jobs = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            tranco_id, domain = line.strip().split(',')
            url = f"{PREFIX}{domain}"
            if url not in worked_urls:
                jobs.append((tranco_id, domain, url))

    with Pool(WORKERS) as p:
        p.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup(TABLE_NAME)
    collect_data(get_absolute_tranco_file_path())


if __name__ == '__main__':
    main()
