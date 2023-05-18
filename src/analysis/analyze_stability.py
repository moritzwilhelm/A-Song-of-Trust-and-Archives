import json
from collections import defaultdict
from datetime import date as date_type, timedelta, datetime
from typing import Callable

from tqdm import tqdm

from analysis.analysis_enums import Status
from analysis.analysis_utils import get_tranco_urls, parse_origin, get_aggregated_date
from analysis.header_utils import normalize_headers, classify_headers
from configs.analysis import RELEVANT_HEADERS
from configs.crawling import INTERNET_ARCHIVE_URL
from configs.database import get_database_cursor
from configs.utils import join_with_json_path, get_absolute_tranco_file_path
from data_collection.collect_archive_data import DATE, TABLE_NAME as ARCHIVE_TABLE_NAME
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def compute_live_data_stability(urls: list[str],
                                start: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MIN'),
                                end: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MAX'),
                                normalization_function: Callable[[dict, str], dict] = normalize_headers) -> None:
    """Compute the stability of (crawled) live data from `start` date up to (inclusive) `end` date."""
    assert start <= end

    live_data = defaultdict(dict)
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT start_url, timestamp::date, headers, end_url 
            FROM {LIVE_TABLE_NAME}
            WHERE status_code=200 AND timestamp::date BETWEEN %s AND %s
        """, (start, end))
        for start_url, date, *data in cursor.fetchall():
            live_data[start_url][date] = data

    result = defaultdict(lambda: defaultdict(dict))
    for url in tqdm(urls):
        seen_values = defaultdict(set)
        result[url] |= {f"USES-{header}": False for header in RELEVANT_HEADERS}
        for date in (start + timedelta(days=i) for i in range((end - start).days + 1)):
            if date not in live_data[url]:
                previous_day = str(date - timedelta(days=1))
                for header in RELEVANT_HEADERS:
                    result[url][f"STABLE-{header}"][str(date)] = result[url][f"STABLE-{header}"].get(previous_day, True)
                continue

            headers, end_url = live_data[url][date]
            for header, value in normalization_function(headers, parse_origin(end_url)).items():
                seen_values[header].add(value)

            for header in RELEVANT_HEADERS:
                result[url][f"STABLE-{header}"][str(date)] = len(seen_values[header]) == 1
                result[url][f"USES-{header}"] |= header in headers

    with open(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}-{normalization_function.__name__}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def compute_archive_snapshot_stability(urls: list[str],
                                       start: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN'),
                                       end: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MAX')) -> None:
    """Compute the stability of archived snapshots from `start` date up to (inclusive) `end` date."""
    assert start <= end

    archive_data = defaultdict(dict)
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT start_url, timestamp::date, end_url, substring(split_part(end_url, '/', 5) FROM 1 FOR 8), status_code 
            FROM {ARCHIVE_TABLE_NAME}
            WHERE status_code IN (200, 404) AND timestamp::date BETWEEN %s AND %s
        """, (start, end))
        for start_url, date, *data in cursor.fetchall():
            archive_data[start_url][date] = data

    result = defaultdict(dict)
    for url in tqdm(urls):
        previous_status = Status.MISSING
        previous_snapshot = None
        start_url = INTERNET_ARCHIVE_URL.format(date=DATE, url=url)
        for date in (start + timedelta(days=i) for i in range((end - start).days + 1)):
            if date not in archive_data[start_url]:
                if previous_status in (Status.ADDED, Status.MODIFIED):
                    status = Status.UNMODIFIED
                elif previous_status == Status.REMOVED:
                    status = Status.MISSING
                else:
                    status = previous_status
            else:
                end_url, day, status_code = archive_data[start_url][date]
                if day not in (datetime.strftime(datetime.strptime(DATE, "%Y%m%d") - timedelta(days=1), '%Y%m%d'),
                               DATE,
                               datetime.strftime(datetime.strptime(DATE, "%Y%m%d") + timedelta(days=1), '%Y%m%d')):
                    if previous_status in (Status.ADDED, Status.MODIFIED):
                        status = Status.UNMODIFIED
                    elif previous_status == Status.REMOVED:
                        status = Status.MISSING
                    else:
                        status = previous_status
                elif status_code == 404:
                    if previous_status in (Status.ADDED, Status.MODIFIED, Status.UNMODIFIED):
                        status = Status.REMOVED
                    else:
                        status = Status.MISSING
                else:
                    if previous_status in (Status.MISSING, Status.REMOVED):
                        status = Status.ADDED
                    else:
                        status = Status.MODIFIED if previous_snapshot != end_url else Status.UNMODIFIED

                    previous_snapshot = end_url

            result[url][str(date)] = status
            previous_status = status

    with open(join_with_json_path(f"STABILITY-{ARCHIVE_TABLE_NAME}-snapshots.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


if __name__ == '__main__':
    # LIVE DATA
    compute_live_data_stability(
        get_tranco_urls(get_absolute_tranco_file_path()),
        normalization_function=normalize_headers
    )

    compute_live_data_stability(
        get_tranco_urls(get_absolute_tranco_file_path()),
        normalization_function=classify_headers
    )

    # ARCHIVE DATA
    compute_archive_snapshot_stability(
        get_tranco_urls(get_absolute_tranco_file_path())
    )

    # compute_archive_snapshot_stability(
    #     get_tranco_urls(get_absolute_tranco_file_path()),
    #     start=get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN') + timedelta(days=1)
    # )
