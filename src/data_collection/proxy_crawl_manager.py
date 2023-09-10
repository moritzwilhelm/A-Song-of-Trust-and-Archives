from argparse import ArgumentParser, Namespace as Arguments
from datetime import datetime, UTC
from multiprocessing import Process, pool, get_context
from subprocess import run

from requests import get

from configs.crawling import TIMESTAMPS, TODAY, SOCKS_PROXIES
from configs.utils import date_range
from data_collection.collect_archive_data import prepare_jobs as prepare_archive_jobs, run_jobs as run_archive_jobs
from data_collection.collect_archive_neighborhoods import crawl_web_archive_cdx, crawl_neighborhoods
from data_collection.collect_contributors import get_archive_sources, prepare_jobs as prepare_metadata_jobs, \
    run_jobs as run_metadata_jobs
from data_collection.crawling import partition_jobs


class NoDaemonProcess(Process):
    """Wrapper class that implements a non-daemonic process."""

    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class NoDaemonContext(type(get_context())):
    Process = NoDaemonProcess


class NoDaemonPool(pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NoDaemonPool, self).__init__(*args, **kwargs)


def parse_args() -> Arguments:
    """Parse command line arguments for starting crawlers distributed over different proxies."""
    parser = ArgumentParser(description='Start archive crawlers and distribute over proxies.')
    parser.add_argument('crawl_type', metavar='<crawl_type>',
                        choices=['test_proxies', 'neighborhood_indexes', 'neighborhoods', 'daily_archive',
                                 'archive_metadata'],
                        help='the type of crawl to start')
    return parser.parse_args()


def build_socks_proxy_configs() -> list[dict[str, str] | None]:
    """Create the set of socks proxy configs."""
    return [dict(http=f"socks5://localhost:{port}",
                 https=f"socks5://localhost:{port}") for port in SOCKS_PROXIES] + [None]


def open_socks_proxies() -> None:
    """Open the socks proxies in the background."""
    for port, remote in SOCKS_PROXIES.items():
        run(['ssh', '-fnN', '-M', '-S', port, '-D', port, '-i', '~/.ssh/Proxies', remote])


def close_socks_proxies() -> None:
    """Close the open socks proxies in the background."""
    for port, remote in SOCKS_PROXIES.items():
        run(['ssh', '-S', port, '-O', 'exit', '-i', '~/.ssh/Proxies', remote])


def test_socks_proxies() -> None:
    """Test availability of socks proxies."""
    response = get('https://api.ipify.org?format=json')
    assert response.status_code == 200, 'IP API is down.'
    ip = response.json()['ip']

    for port, remote in SOCKS_PROXIES.items():
        proxies = dict(http=f"socks5://localhost:{port}", https=f"socks5://localhost:{port}")
        assert get('https://api.ipify.org?format=json', proxies=proxies).json()['ip'] != ip, \
            f"Socks proxy ({port}, {remote}) is broken."


def crawl_web_archive_cdx_worker(timestamps: list[datetime], proxies: dict[str, str] | None) -> None:
    """Worker that initiates the Internet Archive CDX server crawl."""
    crawl_web_archive_cdx(timestamps=timestamps, proxies=proxies)


def start_collect_archive_neighborhood_indexes(configs: list[dict[str, str] | None]) -> None:
    """Start crawling the Internet Archive CDX server with the provided set of proxies."""
    with NoDaemonPool(len(configs)) as nd_pool:
        nd_pool.starmap(crawl_web_archive_cdx_worker, zip(partition_jobs(list(TIMESTAMPS), len(configs)), configs))


def start_collect_archive_neighborhoods(configs: list[dict[str, str] | None]) -> None:
    """Start crawling the Internet Archive CDX server with the provided set of proxies."""
    with NoDaemonPool(len(configs)) as nd_pool:
        nd_pool.starmap(crawl_neighborhoods, zip(partition_jobs(list(TIMESTAMPS), len(configs)), configs))


def crawl_daily_web_archive_worker(timestamps: list[datetime], proxies: dict[str, str] | None) -> None:
    """Worker that initiates the daily Internet Archive crawl."""
    jobs = prepare_archive_jobs(timestamps=timestamps, proxies=proxies)
    run_archive_jobs(jobs)


START_TIMESTAMP = datetime(2023, 7, 16, 12, tzinfo=UTC)


def start_collect_daily_archive_data(configs: list[dict[str, str] | None]) -> None:
    """Start crawling the Internet Archive for all dates in [START_DATE, START_DATE + 14], distributing over proxies."""
    relevant_timestamps = list(filter(lambda date: (TODAY - date).days < 15, date_range(START_TIMESTAMP, TODAY, 15)))
    with NoDaemonPool(len(configs)) as nd_pool:
        nd_pool.starmap(crawl_daily_web_archive_worker, zip(partition_jobs(relevant_timestamps, len(configs)), configs))


def crawl_archive_metadata_api(sources: list[str], proxies: dict[str, str] | None) -> None:
    """Worker that initiates the Internet Archive Metadata crawl."""
    jobs = prepare_metadata_jobs(sources=sources, proxies=proxies)
    run_metadata_jobs(jobs)


def start_collect_contributors(configs: list[dict[str, str] | None]) -> None:
    """Start crawling the Internet Archive Metadata API for all missing sources."""
    with NoDaemonPool(len(configs)) as nd_pool:
        nd_pool.starmap(crawl_archive_metadata_api, zip(partition_jobs(get_archive_sources(), len(configs)), configs))


def main():
    args = parse_args()
    proxy_configs = build_socks_proxy_configs()

    open_socks_proxies()
    try:
        match args.crawl_type:
            case 'test_proxies':
                test_socks_proxies()
            case 'neighborhood_indexes':
                start_collect_archive_neighborhood_indexes(proxy_configs)
            case 'neighborhoods':
                start_collect_archive_neighborhoods(proxy_configs)
            case 'daily_archive':
                start_collect_daily_archive_data(proxy_configs)
            case 'archive_metadata':
                start_collect_contributors(proxy_configs)
    finally:
        close_socks_proxies()


if __name__ == '__main__':
    main()
