from multiprocessing import Pool
from pathlib import Path

from psycopg2 import connect

from configs.crawling import PREFIX
from configs.database import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD
from data_collection.crawling import setup, reset_failed_crawls, crawl

WORKERS = 8

TABLE_NAME = 'live_data'


def worker(urls: list[str]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PWD) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for tranco_id, url in urls:
                success, data = crawl(url)
                if success:
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, end_url, headers, duration, content_hash, status_code) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (tranco_id, url.split('.', 1)[1], url, *data))
                else:
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} 
                        (tranco_id, domain, start_url, headers) 
                        VALUES (%s, %s, %s, to_json(%s::text)::jsonb)
                    """, (tranco_id, url.split('.', 1)[1], url, data))


def collect_data(tranco_file: Path) -> None:
    """Crawl all domains in the `tranco_file`."""
    worked_urls = reset_failed_crawls(TABLE_NAME)

    urls = []
    with open(tranco_file) as file:
        for line in file:
            tranco_id, domain = line.strip().split(',')
            url = f"{PREFIX}{domain}"
            if url not in worked_urls:
                urls.append((tranco_id, url))

    chunks = [urls[i:i + len(urls) // WORKERS] for i in range(0, len(urls), len(urls) // WORKERS)]

    with Pool(WORKERS) as p:
        p.map(worker, chunks)


def main():
    setup(TABLE_NAME)
    collect_data(Path(__file__).parents[1].resolve().joinpath('configs', 'tranco_20k.csv'))


if __name__ == '__main__':
    main()
