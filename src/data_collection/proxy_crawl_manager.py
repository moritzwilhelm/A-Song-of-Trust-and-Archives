from argparse import ArgumentParser, Namespace as Arguments
from multiprocessing import Pool
from typing import List, Dict, Optional

from configs.crawling import TIMESTAMPS
from data_collection.collect_archive_proximity_set import crawl_web_archive_cdx
from data_collection.crawling import partition_jobs


def build_proxies(ports: List[int]) -> List[Optional[Dict[str, str]]]:
    """Create the set of proxies based on the provided `ports`."""
    return [dict(http=f"socks5://localhost:{port}", https=f"socks5://localhost:{port}") for port in ports] + [None]


def parse_args() -> Arguments:
    """Parse command line arguments for starting crawlers distributed over different proxies."""
    parser = ArgumentParser(description='Start archive crawlers and distribute over .')
    parser.add_argument('crawl_type', metavar='<crawl_type>', choices=['proximity_set_indexes'],
                        help='the type of crawl to start')
    parser.add_argument('-p', '--ports', metavar='<port>', type=int, nargs='*', default=set(),
                        help='an open socks proxy port')
    return parser.parse_args()


def crawl_web_archive_cdx_worker(timestamps, proxy):
    """Worker that initiates the Internet Archive CDX server crawl."""
    return crawl_web_archive_cdx(timestamps=timestamps, proxies=proxy)


def start_collect_archive_proximity_set_indexes(proxies: List[Optional[Dict[str, str]]]) -> None:
    """Start crawling the Internet Archive CDX server with the provided set of proxies."""
    with Pool(len(proxies)) as pool:
        pool.starmap(crawl_web_archive_cdx_worker, zip(partition_jobs(list(TIMESTAMPS), len(proxies)), proxies))


def main():
    args = parse_args()
    proxies = build_proxies(args.ports)

    if args.crawl_type == 'proximity_set_indexes':
        start_collect_archive_proximity_set_indexes(proxies)


if __name__ == '__main__':
    main()
