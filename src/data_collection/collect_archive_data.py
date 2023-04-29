import time
from multiprocessing import Pool
from pathlib import Path

import psycopg2 as psycopg2
import requests

from configs.crawling import PREFIX, APIs
from configs.database import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD
from data_collection.crawling import crawl, setup

WORKERS = 8

DATE = "20230428"
TABLE_NAME = f"archive_data_{DATE}"
ARCHIVE_URL = APIs['archiveorg']
ARCHIVE_URL_LENGTH = len(ARCHIVE_URL.format(date=DATE, url=f"{PREFIX}"))


def worker(urls):
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            session = requests.Session()
            for id_, url in urls:
                time.sleep(0.2)
                success, data = crawl(url, session=session)
                if success:
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (id_, url[ARCHIVE_URL_LENGTH:], url, *data))
                else:
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, end_url) 
                        VALUES (%s, %s, %s, %s)
                    """, (id_, url[ARCHIVE_URL_LENGTH:], url, data))


def collect_data(tranco_file):
    # reset failed attempts
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"""
                DELETE FROM {TABLE_NAME} WHERE timestamp::date = 'today' and status_code IS NULL OR status_code = 429
            """)
            cursor.execute(f"SELECT start_url FROM {TABLE_NAME} WHERE timestamp::date = 'today'")
            worked_urls = {x[0] for x in cursor.fetchall()}

    urls = []
    with open(tranco_file) as file:
        for line in file:
            id_, domain = line.strip().split(',')
            url = ARCHIVE_URL.format(date=DATE, url=f"{PREFIX}{domain}")
            if url in worked_urls:
                continue
            urls.append((id_, url))

    chunks = [urls[i:i + len(urls) // WORKERS] for i in range(0, len(urls), len(urls) // WORKERS)]

    with Pool(WORKERS) as p:
        p.map(worker, chunks)


def main():
    setup(TABLE_NAME)
    collect_data(f'{Path(__file__).parent.resolve()}/tranco_20k.csv')


if __name__ == '__main__':
    main()
