from hashlib import sha256
from multiprocessing import Pool
from typing import NamedTuple

from bs4 import BeautifulSoup

from analysis.live.analyze_stability import ARCHIVE_TABLE_NAME as ARCHIVE_TABLE_FOR_STABILITY
from configs.database import STORAGE, get_database_cursor
from configs.files.random_sample_tranco import RANDOM_SAMPLING_TABLE_NAME
from data_collection.collect_archive_data import TABLE_NAME as ARCHIVE_TABLE_NAME
from data_collection.collect_archive_proximity_sets import TABLE_NAME as PROXIMITY_SETS_TABLE_NAME
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from data_collection.crawling import normalize_archived_content, partition_jobs

WORKERS = 128
RELAXED_HASHES_TABLE_NAME = 'RELAXED_HASHES'


class HashJob(NamedTuple):
    """Represents a job for computing the relaxed hash of an HTML file."""
    content_hash: str
    is_archived_response: bool


def setup_relaxed_hashes_table() -> None:
    """Create relaxed hashes database table and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {RELAXED_HASHES_TABLE_NAME} (
                content_hash VARCHAR(64) PRIMARY KEY,
                relaxed_hash VARCHAR(64)
            );
        """)

        for column in ['content_hash', 'relaxed_hash']:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {RELAXED_HASHES_TABLE_NAME}_{column}_idx 
                ON {RELAXED_HASHES_TABLE_NAME} ({column})
            """)


def worker(content_hashes: list[HashJob]) -> None:
    with get_database_cursor(autocommit=True) as cursor:
        for content_hash, is_archived_response in content_hashes:
            with open(STORAGE.joinpath(content_hash[0], content_hash[1], f"{content_hash}.gz"), 'rb') as file:
                content = normalize_archived_content(file.read()) if is_archived_response else file.read()

            content = BeautifulSoup(content, 'html5lib').prettify().encode()
            relaxed_hash = sha256(content).hexdigest()

            cursor.execute(f"""
                INSERT INTO {RELAXED_HASHES_TABLE_NAME} (content_hash, relaxed_hash)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (content_hash, relaxed_hash))


def prepare_jobs(table_name: str, is_archive_table: bool) -> list[HashJob]:
    """Generate HashJob list for all missing content_hashes in `table_name`."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT DISTINCT t.content_hash 
            FROM {table_name} t LEFT JOIN {RELAXED_HASHES_TABLE_NAME} r USING (content_hash)
            WHERE t.content_hash IS NOT NULL AND r.content_hash IS NULL
        """)

        return [HashJob(content_hash, is_archive_table) for content_hash, in cursor.fetchall()]


def run_jobs(jobs: list[HashJob]) -> None:
    """Execute the provided HashJobs using multiprocessing."""
    with Pool(WORKERS) as pool:
        pool.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup_relaxed_hashes_table()

    # Prepare and execute the hash jobs
    jobs = [
        *prepare_jobs(LIVE_TABLE_NAME, False),
        *prepare_jobs(ARCHIVE_TABLE_NAME, True),
        *prepare_jobs(RANDOM_SAMPLING_TABLE_NAME, True),
        *prepare_jobs(f"{ARCHIVE_TABLE_NAME}_FOR_COMPARISON", True),
        *prepare_jobs(ARCHIVE_TABLE_FOR_STABILITY, True),
        *prepare_jobs(PROXIMITY_SETS_TABLE_NAME, True)
    ]
    run_jobs(jobs)


if __name__ == '__main__':
    main()
