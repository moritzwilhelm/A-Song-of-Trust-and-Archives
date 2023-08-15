import gzip
import random
import re
import signal
import traceback
from collections import defaultdict
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, date as date_type
from hashlib import sha256
from itertools import cycle
from time import time_ns, sleep
from types import FrameType

from psycopg2.extras import Json
from requests import Session, RequestException, Response

from configs.analysis import MEMENTO_HEADER
from configs.crawling import USER_AGENT, TODAY, WAYBACK_HEADER_REGEX, WAYBACK_TOOLBAR_REGEX, WAYBACK_COMMENTS_REGEX, \
    WAYBACK_MACHINE_API_PATH
from configs.database import STORAGE, get_database_cursor


def setup(table_name: str) -> None:
    """Create crawling database table and create relevant indexes."""
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                tranco_id INTEGER,
                domain VARCHAR(128),
                timestamp TIMESTAMPTZ DEFAULT NULL,
                start_url VARCHAR(128),
                end_url TEXT DEFAULT NULL,
                status_code INT DEFAULT NULL,
                headers JSONB DEFAULT NULL,
                content_hash VARCHAR(64) DEFAULT NULL,
                response_time NUMERIC DEFAULT NULL,
                crawl_datetime TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        for column in ['tranco_id', 'domain', 'timestamp', 'start_url', 'end_url', 'status_code', 'content_hash',
                       'response_time', 'crawl_datetime']:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_{column}_idx ON {table_name} ({column})")


def reset_failed_archive_crawls(table_name: str, date: date_type = TODAY.date()) -> dict[datetime, set[int]]:
    """Delete all crawling results that are missing the memento header, except for responses with status code 404."""
    with get_database_cursor(autocommit=True) as cursor:
        cursor.execute(f"""
            DELETE FROM {table_name}
            WHERE crawl_datetime::date=%s AND (status_code IS NULL OR headers->>%s IS NULL AND status_code!=404)
        """, (date, MEMENTO_HEADER.lower()))

        cursor.execute(f"""
            SELECT timestamp, ARRAY_AGG(tranco_id) FROM {table_name} WHERE crawl_datetime::date=%s GROUP BY timestamp
        """, (date,))
        return defaultdict(set, set(cursor.fetchall()))


def partition_jobs(jobs: list, n: int) -> list[list]:
    """Partition list of jobs into `n` partitions of (almost) equal size."""
    partition = [[] for _ in range(n)]

    random.shuffle(jobs)
    for i, job in zip(cycle(range(n)), jobs):
        partition[i].append(job)

    return partition


@contextmanager
def timeout(seconds: int) -> Generator[None, None, None]:
    """Wrapper that throws a TimeoutError after `seconds` seconds."""

    def raise_timeout(signal_number: int, frame: FrameType | None) -> None:
        raise TimeoutError(f"Hard kill due to signal timeout ({seconds}s)!")

    # Register a function to raise a TimeoutError on the signal.
    signal.signal(signal.SIGALRM, raise_timeout)
    # Schedule the signal to be sent after the specified seconds.
    signal.alarm(seconds)
    try:
        yield
    finally:
        # Unregister the signal, so it won't be triggered, if the timeout is not reached.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)


def remove_wayback_toolbar(content: bytes) -> bytes:
    """Remove injected JS in <head>, toolbar in <body>, and comment after </html>."""
    content = re.sub(WAYBACK_HEADER_REGEX, b'<head>', content)
    content = re.sub(WAYBACK_TOOLBAR_REGEX, b'', content)
    return re.sub(WAYBACK_COMMENTS_REGEX, b'', content)


def store_on_disk(content: bytes) -> str:
    """Store the provided content on the disk and compute the content hash."""
    content_hash = sha256(content).hexdigest()
    file_path = STORAGE.joinpath(content_hash[0], content_hash[1], f"{content_hash}.gz")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(file_path, 'wb') as fh:
        fh.write(content)

    return content_hash


class CrawlingException(Exception):
    """An Exception to be raised if there has been an error during crawling."""

    def to_json(self) -> Json:
        return Json(dict(
            error=str(self),
            cause=type(self.__cause__).__name__ if self.__cause__ else None,
            message=str(self.__cause__),
            traceback=''.join(traceback.format_tb(self.__cause__.__traceback__)) if self.__cause__ is not None else None
        ))


class CrawlingResponse(Response):
    """A wrapper clas that extends requests' Response object with a `content_hash` and `response_time`."""
    __attrs__ = Response.__attrs__ + ["content_hash", "response_time", "serialized_data"]

    def __init__(self, response: Response, content_hash: str, response_time: int):
        super().__init__()
        self.__dict__ = response.__dict__
        self.content_hash = content_hash
        self.response_time = response_time

    @property
    def serialized_data(self) -> tuple[str, int, Json, str, int]:
        return self.url, self.status_code, Json(dict(self.headers)), self.content_hash, self.response_time


def crawl(url: str,
          headers: dict[str, str] | None = None,
          user_agent: str = USER_AGENT,
          proxies: dict[str, str] | None = None,
          session: Session | None = None) -> CrawlingResponse:
    """Crawl the URL, using the provided `headers`, `user_agent`, `session` (if specified).

    Return the response object and its hashed content. Raise a CrawlingException on failure.
    """
    if session is None:
        session = Session()
    if headers is None:
        headers = dict()
    headers['User-Agent'] = user_agent

    try:
        with timeout(30):
            start = time_ns()
            response = session.get(url, headers=headers, proxies=proxies, timeout=20)
            response_time = time_ns() - start
    except (RequestException, TimeoutError) as error:
        raise CrawlingException(url) from error

    # store content on disk
    content = remove_wayback_toolbar(response.content) if url.startswith(WAYBACK_MACHINE_API_PATH) else response.content
    content_hash = store_on_disk(content)

    # mitigate rate-limiting
    if response.status_code == 429:
        print("WARNING: RATE-LIMITING - 429")
        sleep(10)

    return CrawlingResponse(response, content_hash, response_time)
