from multiprocessing import Pool
from pathlib import Path

from configs.crawling import PREFIX
from configs.database import get_database_cursor
from data_collection.crawling import setup, reset_failed_crawls, crawl

WORKERS = 8

TABLE_NAME = 'live_data'


def worker(urls: list[str]) -> None:
    """Crawl all provided `urls` and store the responses in the database."""
    with get_database_cursor(autocommit=True) as cursor:
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
