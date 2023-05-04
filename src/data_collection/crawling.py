import gzip
import json
import os
import signal
from contextlib import contextmanager
from hashlib import sha256
from time import time_ns

import psycopg2
from requests import Session, RequestException

from configs.crawling import USER_AGENT
from configs.database import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD, STORAGE


def setup(table_name):
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    tranco_id INTEGER,
                    domain VARCHAR(128),
                    start_url VARCHAR(128),
                    end_url TEXT DEFAULT NULL,
                    headers JSONB DEFAULT NULL,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    duration NUMERIC DEFAULT NULL,
                    content_hash VARCHAR(64) DEFAULT NULL,
                    status_code INT DEFAULT NULL
                );
            """)

            for column in ['tranco_id', 'domain', 'start_url', 'end_url', 'timestamp', 'duration', 'content_hash',
                           'status_code']:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {table_name}_{column}_idx ON {table_name} ({column})")


def reset_failed_crawls(table_name):
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {table_name} WHERE timestamp::date = 'today' and status_code IS NULL OR status_code = 429"
            )
            cursor.execute(f"SELECT start_url FROM {table_name} WHERE timestamp::date = 'today'")
            return {x[0] for x in cursor.fetchall()}


@contextmanager
def timeout(seconds):
    def raise_timeout(signum, frame):
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


def crawl(url, headers=None, user_agent=USER_AGENT, session=None):
    if session is None:
        session = Session()
    if headers is None:
        headers = dict()
    headers['User-Agent'] = user_agent

    try:
        with timeout(30):
            start = time_ns()
            response = session.get(url, headers=headers, timeout=20)
            duration = time_ns() - start
    except RequestException as error:
        return False, str(error)
    except TimeoutError as error:
        return False, str(error)

    # store content on disk
    content = response.content
    content_hash = sha256(content).hexdigest()
    file_dir = os.path.join(STORAGE, content_hash[0], content_hash[1])
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    file_path = os.path.join(file_dir, f"{content_hash}.gz")
    with gzip.open(file_path, 'wb') as fh:
        fh.write(content)

    # normalize header names
    response_headers = json.dumps({h.lower(): response.headers[h] for h in response.headers})

    return True, (response.url, response_headers, duration, content_hash, response.status_code)
