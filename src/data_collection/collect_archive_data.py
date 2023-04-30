import time
from multiprocessing import Pool
from pathlib import Path

import psycopg2 as psycopg2
import requests

from configs.crawling import PREFIX, APIs
from configs.database import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD
from data_collection.crawling import setup, reset_failed_crawls, crawl

WORKERS = 8

DATE = '20230501'
TABLE_NAME = f"archive_data_{DATE}"
ARCHIVE_URL = APIs['archiveorg']
ARCHIVE_URL_LENGTH = len(ARCHIVE_URL.format(date=DATE, url=f"{PREFIX}"))


def worker(urls):
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            session = requests.Session()
            for tranco_id, url in urls:
                time.sleep(0.2)
                success, data = crawl(url, session=session)
                if success:
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (tranco_id, url[ARCHIVE_URL_LENGTH:], url, *data))
                else:
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, headers) 
                        VALUES (%s, %s, %s, to_json(%s::text)::jsonb)
                    """, (tranco_id, url[ARCHIVE_URL_LENGTH:], url, data))


def collect_data(tranco_file):
    worked_urls = reset_failed_crawls(TABLE_NAME)

    urls = []
    with open(tranco_file) as file:
        for line in file:
            tranco_id, domain = line.strip().split(',')
            url = ARCHIVE_URL.format(date=DATE, url=f"{PREFIX}{domain}")
            if url not in worked_urls:
                urls.append((tranco_id, url))

    chunks = [urls[i:i + len(urls) // WORKERS] for i in range(0, len(urls), len(urls) // WORKERS)]

    with Pool(WORKERS) as p:
        p.map(worker, chunks)


def main():
    setup(TABLE_NAME)
    collect_data(Path(__file__).parent.resolve().joinpath('tranco_20k.csv'))


if __name__ == '__main__':
    main()
