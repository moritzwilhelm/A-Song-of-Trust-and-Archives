from argparse import ArgumentParser, Namespace as Arguments
from datetime import datetime
from multiprocessing import Process, pool
from typing import List, Dict, Optional

from pytz import utc

from configs.crawling import TIMESTAMPS, TODAY
from configs.utils import date_range
from data_collection.collect_archive_data import prepare_jobs as prepare_archive_jobs, run_jobs as run_archive_jobs
from data_collection.collect_archive_proximity_sets import crawl_web_archive_cdx, crawl_proximity_sets
from data_collection.crawling import partition_jobs


class NoDaemonProcess(Process):
    """Wrapper class that implements a non-daemonic process."""

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class NoDaemonPool(pool.Pool):
    """Wrapper class to allow pool processes to spawn children."""
    Process = NoDaemonProcess


def parse_args() -> Arguments:
    """Parse command line arguments for starting crawlers distributed over different proxies."""
    parser = ArgumentParser(description='Start archive crawlers and distribute over proxies.')
    parser.add_argument('crawl_type', metavar='<crawl_type>',
                        choices=['proximity_set_indexes', 'proximity_sets', 'daily_archive'],
                        help='the type of crawl to start')
    parser.add_argument('-p', '--ports', metavar='<port>', type=int, nargs='*', default=set(),
                        help='an open socks proxy port')
    return parser.parse_args()


def build_proxies(ports: List[int]) -> List[Optional[Dict[str, str]]]:
    """Create the set of proxies based on the provided `ports`."""
    return [dict(http=f"socks5://localhost:{port}", https=f"socks5://localhost:{port}") for port in ports] + [None]


def crawl_web_archive_cdx_worker(timestamps: List[datetime], proxies: Optional[Dict[str, str]]) -> None:
    """Worker that initiates the Internet Archive CDX server crawl."""
    return crawl_web_archive_cdx(timestamps=timestamps, proxies=proxies)


def start_collect_archive_proximity_set_indexes(proxies: List[Optional[Dict[str, str]]]) -> None:
    """Start crawling the Internet Archive CDX server with the provided set of proxies."""
    with NoDaemonPool(len(proxies)) as nd_pool:
        nd_pool.starmap(crawl_web_archive_cdx_worker, zip(partition_jobs(list(TIMESTAMPS), len(proxies)), proxies))


def start_collect_archive_proximity_sets(proxies: List[Optional[Dict[str, str]]]) -> None:
    """Start crawling the Internet Archive CDX server with the provided set of proxies."""
    with NoDaemonPool(len(proxies)) as nd_pool:
        nd_pool.starmap(crawl_proximity_sets, zip(partition_jobs(list(TIMESTAMPS), len(proxies)), proxies))


def crawl_daily_web_archive_worker(timestamps: List[datetime], proxies: Optional[Dict[str, str]]) -> None:
    """Worker that initiates the daily Internet Archive crawl."""
    jobs = prepare_archive_jobs(timestamps=timestamps, proxies=proxies)
    return run_archive_jobs(jobs)


START_TIMESTAMP = datetime(2023, 7, 1, 12, tzinfo=utc)


def start_collect_daily_archive_data(proxies: List[Optional[Dict[str, str]]]) -> None:
    """Start crawling the Internet Archive for all dates in [START_DATE, START_DATE + 14], distributing over proxies."""
    relevant_timestamps = list(filter(lambda date: (TODAY.date() - date).days < 15,
                                      date_range(START_TIMESTAMP.date(), TODAY.date(), 15)))
    with NoDaemonPool(len(proxies)) as nd_pool:
        nd_pool.starmap(crawl_daily_web_archive_worker, zip(partition_jobs(relevant_timestamps, len(proxies)), proxies))


def main():
    args = parse_args()
    proxies = build_proxies(args.ports)

    if args.crawl_type == 'proximity_set_indexes':
        start_collect_archive_proximity_set_indexes(proxies)
    elif args.crawl_type == 'proximity_sets':
        start_collect_archive_proximity_sets(proxies)
    elif args.crawl_type == 'daily_archive':
        start_collect_daily_archive_data(proxies)


if __name__ == '__main__':
    main()
