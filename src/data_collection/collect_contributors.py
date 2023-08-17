from multiprocessing import Pool
from time import sleep
from typing import NamedTuple

import requests
from psycopg2.extras import Json
from requests import JSONDecodeError, Session

from configs.analysis import INTERNET_ARCHIVE_SOURCE_HEADER
from configs.crawling import INTERNET_ARCHIVE_METADATA_API
from configs.database import get_database_cursor
from data_collection.collect_archive_proximity_sets import TABLE_NAME as PROXIMITY_SETS_TABLE_NAME
from data_collection.crawling import partition_jobs, crawl, CrawlingException

WORKERS = 8

METADATA_TABLE_NAME = 'historical_data_metadata'


class MetadataJob(NamedTuple):
    """Represents a job for crawling the IA metadata API and storing data in the database."""
    source: str
    proxies: dict[str, str] | None = None


def setup_metadata_table() -> None:
    """Create database table for archive data source metadata and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                source VARCHAR(256) PRIMARY KEY,
                raw_data JSONB DEFAULT NULL,
                metadata JSONB DEFAULT NULL,
                contributor VARCHAR(64) DEFAULT NULL,
                error JSONB DEFAULT NULL,
                crawl_datetime TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        for column in ['source', 'metadata', 'contributor', 'error', 'crawl_datetime']:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {METADATA_TABLE_NAME}_{column}_idx ON {METADATA_TABLE_NAME} ({column})
            """)


def get_archive_sources() -> list[str]:
    """Retrieve the set of archive sources form the database."""
    with get_database_cursor(autocommit=True) as cursor:
        cursor.execute(f"""
            SELECT DISTINCT SPLIT_PART(headers->>%s, '/', 1)
            FROM {PROXIMITY_SETS_TABLE_NAME}
            WHERE headers->>%s IS NOT NULL
        """, (INTERNET_ARCHIVE_SOURCE_HEADER.lower(), INTERNET_ARCHIVE_SOURCE_HEADER.lower()))
        return [source for source, in cursor.fetchall()]


def crawl_metadata(source: str,
                   proxies: dict[str, str] | None = None,
                   session: Session | None = None) -> tuple[Json, Json, str]:
    """Crawl the Internet Archive Metadata API for the provided source."""
    response = crawl(INTERNET_ARCHIVE_METADATA_API.format(source=source), proxies=proxies, session=session)

    try:
        data = response.json()
    except JSONDecodeError as error:
        raise CrawlingException(response.url) from error

    return Json(data), Json(data.get('metadata')), data.get('metadata', {}).get('contributor')


def worker(jobs: list[MetadataJob], table_name=METADATA_TABLE_NAME) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
        session = requests.Session()
        for source, proxies in jobs:
            sleep(0.2)
            try:
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    (source, raw_data, metadata, contributor)
                    VALUES (%s, %s, %s, %s)
                """, (source, *crawl_metadata(source, proxies=proxies, session=session)))
            except CrawlingException as error:
                cursor.execute(f"""
                    INSERT INTO {table_name}
                    (source, error)
                    VALUES (%s, %s)
                """, (source, error.to_json()))


def reset_failed_metadata_crawls() -> set[str]:
    """Delete all crawling results with an error."""
    with get_database_cursor(autocommit=True) as cursor:
        cursor.execute(f"DELETE FROM {METADATA_TABLE_NAME} WHERE error IS NOT NULL")
        cursor.execute(f"SELECT DISTINCT source FROM {METADATA_TABLE_NAME}")
        return {source for source, in cursor.fetchall()}


def prepare_jobs(sources: list[str], proxies: dict[str, str] | None = None) -> list[MetadataJob]:
    """Generate MetadataJob list for all unique sources in the database."""
    worked_jobs = reset_failed_metadata_crawls()
    return [MetadataJob(source, proxies) for source in sources if source not in worked_jobs]


def run_jobs(jobs: list[MetadataJob]) -> None:
    """Execute the provided crawl jobs using multiprocessing."""
    with Pool(WORKERS) as pool:
        pool.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup_metadata_table()

    # Prepare and execute the crawl jobs
    jobs = prepare_jobs(get_archive_sources())
    run_jobs(jobs)


if __name__ == '__main__':
    main()
