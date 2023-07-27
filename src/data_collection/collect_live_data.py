from datetime import date as date_type
from multiprocessing import Pool
from pathlib import Path
from typing import NamedTuple, List, Set

from configs.crawling import NUMBER_URLS, TODAY
from configs.database import get_database_cursor
from configs.utils import get_absolute_tranco_file_path, get_tranco_data
from data_collection.crawling import setup, partition_jobs, CrawlingException, crawl

WORKERS = 8

TABLE_NAME = 'LIVE_DATA'


class LiveJob(NamedTuple):
    """Represents a job for crawling and storing data in the database."""
    tranco_id: int
    domain: str
    url: str


def worker(jobs: List[LiveJob], table_name=TABLE_NAME) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        for tranco_id, domain, url in jobs:
            try:
                response = crawl(url)
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    (tranco_id, domain, timestamp, start_url, end_url, status_code, headers, content_hash, response_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, domain, TODAY, url, *response.serialized_data))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    (tranco_id, domain, timestamp, start_url, headers)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tranco_id, domain, TODAY, url, error.to_json()))


def reset_failed_crawls(table_name: str, date: date_type = TODAY.date()) -> Set[int]:
    """Delete all crawling results whose status code is 429 or NULL and return the affected tranco ids."""
    with get_database_cursor(autocommit=True) as cursor:
        cursor.execute(f"""
            DELETE FROM {table_name} WHERE crawl_datetime::date=%s AND (status_code IS NULL OR status_code=429)
        """, (date,))

        cursor.execute(f"SELECT DISTINCT tranco_id FROM {table_name} WHERE crawl_datetime::date=%s", (date,))
        return {tid for tid, in cursor.fetchall()}


def prepare_jobs(tranco_file: Path = get_absolute_tranco_file_path(), n: int = NUMBER_URLS) -> List[LiveJob]:
    """Build a list of LiveJob instances for the given Tranco file and maximum number of domains."""
    worked_jobs = reset_failed_crawls(TABLE_NAME)
    return [LiveJob(tid, domain, url) for tid, domain, url in get_tranco_data(tranco_file, n) if tid not in worked_jobs]


def run_jobs(jobs: List[LiveJob]) -> None:
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
