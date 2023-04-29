from multiprocessing import Pool
from pathlib import Path

import psycopg2

from configs.crawling import PREFIX
from configs.database import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD
from crawling import setup, crawl

WORKERS = 8

TABLE_NAME = 'live_data'


def worker(id_, url):
    success, data = crawl(url)
    with psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            if success:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} 
                    (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (id_, url.split('.', 1)[1], url, *data))
            else:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} 
                    (tranco_id, domain, start_url, end_url) 
                    VALUES (%s, %s, %s, %s)
                """, (id_, url.split('.', 1)[1], url, data))


def collect_data(tranco_file):
    urls = []
    with open(tranco_file) as file:
        for line in file:
            id_, domain = line.strip().split(',')
            url = f"{PREFIX}{domain}"
            urls.append((id_, url))

    with Pool(WORKERS) as p:
        p.starmap(worker, urls)


def main():
    setup(TABLE_NAME)
    collect_data(f'{Path(__file__).parent.resolve()}/tranco_20k.csv')


if __name__ == '__main__':
    main()
