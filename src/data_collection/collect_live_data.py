from itertools import islice
from multiprocessing import Pool
from pathlib import Path
from typing import NamedTuple, List

from configs.crawling import PREFIX
from configs.database import get_database_cursor, setup
from configs.utils import get_absolute_tranco_file_path
from data_collection.crawling import reset_failed_crawls, partition_jobs, crawl, CrawlingException

WORKERS = 8

TABLE_NAME = 'live_data'


class LiveJob(NamedTuple):
    """Represents a job for crawling and storing data in the database."""
    tranco_id: int
    domain: str
    url: str


def worker(jobs: List[LiveJob]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        for tranco_id, domain, url in jobs:
            try:
                response = crawl(url)
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} 
                    (tranco_id, domain, start_url, end_url, status_code, headers, content_hash, response_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (tranco_id, domain, url, *response.serialized_data))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} 
                    (tranco_id, domain, start_url, headers) 
                    VALUES (%s, %s, %s, %s)
                """, (tranco_id, domain, url, error.to_json()))


def prepare_jobs(tranco_file: Path, n: int = 20000) -> List[LiveJob]:
    """Build a list of LiveJob instances for the given Tranco file and maximum number of domains."""
    worked_urls = reset_failed_crawls(TABLE_NAME)

    jobs = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            tranco_id, domain = line.strip().split(',')
            url = f"{PREFIX}{domain}"
            if url not in worked_urls:
                jobs.append(LiveJob(tranco_id, domain, url))

    return jobs


def run_jobs(jobs: List[LiveJob]) -> None:
    """Execute the provided crawl jobs using multiprocessing."""
    with Pool(WORKERS) as p:
        p.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup(TABLE_NAME)

    # Prepare and execute the crawl jobs
    jobs = prepare_jobs(get_absolute_tranco_file_path())
    run_jobs(jobs)


if __name__ == '__main__':
    main()
