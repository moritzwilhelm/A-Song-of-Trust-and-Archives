import gzip
import re
from multiprocessing import Pool
from typing import NamedTuple, Callable
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from psycopg2.extras import Json

from analysis.analysis_utils import parse_hostname, parse_site, is_disconnect_tracker, \
    is_easyprivacy_tracker
from analysis.header_utils import parse_origin
from configs.database import STORAGE, get_database_cursor
from data_collection.collect_archive_data import TABLE_NAME as ARCHIVE_TABLE_NAME
from data_collection.collect_archive_neighborhoods import TABLE_NAME as NEIGHBORHOODS_TABLE_NAME
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from data_collection.crawling import partition_jobs

WORKERS = 128

METADATA_TABLE_NAME = 'HTML_SCRIPT_METADATA'

INTERNET_ARCHIVE_SOURCE_REGEX = r"https?://web\.archive\.org/web/\d+(js_)?/(https?://.*)"


class AnalysisJob(NamedTuple):
    """Represents a job for analyzing the set of included scripts in an HTML file."""
    content_hash: str
    end_url: str
    sources_filter: Callable[[set[str]], set[str]]


def setup_metadata_table() -> None:
    """Create HTML script metadata database table and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (
                content_hash VARCHAR(64) PRIMARY KEY,
                sources JSONB,
                relevant_sources JSONB,
                hosts JSONB,
                sites JSONB,
                disconnect_trackers JSONB,
                easyprivacy_trackers JSONB,
                analysis_datetime TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        for column in ['content_hash', 'hosts', 'sites']:
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {METADATA_TABLE_NAME}_{column}_idx ON {METADATA_TABLE_NAME} ({column})
            """)


def live_sources_filter(sources: set[str]) -> set[str]:
    """Collect all sources."""
    return {source for source in sources if re.match(r"https?://.*", source) is not None}


def archive_sources_filter(sources: set[str]) -> set[str]:
    """Collect all original sources mirrored by the Internet Archive."""
    matched_sources = (re.match(INTERNET_ARCHIVE_SOURCE_REGEX, source) for source in sources)
    return {source.group(2) for source in matched_sources if source is not None}


def worker(jobs: list[AnalysisJob]) -> None:
    """Extract all hosts/sites included in the given HTML document."""
    with get_database_cursor(autocommit=True) as cursor:
        for content_hash, end_url, sources_filter in jobs:
            with gzip.open(STORAGE.joinpath(content_hash[0], content_hash[1], f"{content_hash}.gz")) as file:
                soup = BeautifulSoup(file.read(), 'html5lib')

            sources = {urljoin(end_url, script_element.get('src')) for script_element in soup.select('script[src]')}
            relevant_sources = sources_filter(sources)

            hosts = {parse_hostname(source) for source in relevant_sources}
            sites = {parse_site(source) for source in relevant_sources}
            disconnect_trackers = {source for source in relevant_sources
                                   if is_disconnect_tracker(source, parse_origin(end_url))}
            easyprivacy_trackers = {source for source in relevant_sources
                                    if is_easyprivacy_tracker(source, parse_origin(end_url))}

            cursor.execute(f"""
                INSERT INTO {METADATA_TABLE_NAME}
                (content_hash, sources, relevant_sources, hosts, sites, disconnect_trackers, easyprivacy_trackers)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (content_hash, Json(sorted(sources)), Json(sorted(relevant_sources)), Json(sorted(hosts)),
                  Json(sorted(sites)), Json(sorted(disconnect_trackers)), Json(sorted(easyprivacy_trackers))))


def prepare_jobs(table_name: str,
                 sources_filter: Callable[[set[str]], set[str]] = archive_sources_filter) -> list[AnalysisJob]:
    """Generate AnalysisJob list for all missing content_hashes in `table_name`."""
    with get_database_cursor(autocommit=True) as cursor:
        cursor.execute(f"""
            SELECT t.content_hash, t.end_url
            FROM {table_name} t
            LEFT JOIN {METADATA_TABLE_NAME} m USING (content_hash)
            WHERE t.content_hash IS NOT NULL AND m.content_hash IS NULL
        """)

        return [AnalysisJob(*data, sources_filter=sources_filter) for data in cursor.fetchall()]


def run_jobs(jobs: list[AnalysisJob]) -> None:
    """Execute the provided AnalysisJobs using multiprocessing."""
    with Pool(WORKERS) as pool:
        pool.map(worker, partition_jobs(jobs, WORKERS))


def main():
    setup_metadata_table()

    # Prepare and execute the analysis jobs
    jobs = [
        *prepare_jobs(LIVE_TABLE_NAME, live_sources_filter),
        *prepare_jobs(ARCHIVE_TABLE_NAME),
        *prepare_jobs(NEIGHBORHOODS_TABLE_NAME)
    ]
    run_jobs(jobs)


if __name__ == '__main__':
    main()
