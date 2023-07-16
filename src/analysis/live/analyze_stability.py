import json
from collections import defaultdict
from datetime import date as date_type, timedelta, datetime
from typing import Callable, List, Tuple, Optional

from pytz import utc
from tqdm import tqdm

from analysis.analysis_utils import parse_origin, get_aggregated_date
from analysis.header_utils import Headers, normalize_headers, classify_headers
from analysis.live.stability_enums import Status
from configs.analysis import RELEVANT_HEADERS, MEMENTO_HEADER, MEMENTO_HEADER_FORMAT
from configs.database import get_database_cursor
from configs.utils import join_with_json_path, get_tranco_data, date_range
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def analyze_live_data(targets: List[Tuple[int, str, str]],
                      start: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MIN'),
                      end: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MAX'),
                      aggregation_function: Callable[[Headers, Optional[str]], Headers] = normalize_headers) -> None:
    """Compute the stability of (crawled) live data from `start` date up to (inclusive) `end` date."""
    assert start <= end

    live_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp::date, headers, end_url
            FROM {LIVE_TABLE_NAME}
            WHERE status_code=200 AND timestamp::date BETWEEN %s AND %s
        """, (start, end))
        for tid, date, headers, end_url in cursor.fetchall():
            live_data[tid, date] = (headers, aggregation_function(headers, parse_origin(end_url)))

    result = {tid: {header: {'DEPLOYS': False} for header in RELEVANT_HEADERS} for tid, _, _ in targets}
    for tid, _, _ in tqdm(targets):
        seen_values = defaultdict(set)
        for date in date_range(start, end):
            if (tid, date) in live_data:
                headers, aggregated_headers = live_data[tid, date]
                for header in RELEVANT_HEADERS:
                    seen_values[header].add(aggregated_headers[header])
                    result[tid][header][str(date)] = len(seen_values[header]) == 1
                    result[tid][header]['DEPLOYS'] |= header in headers
            else:
                previous_timestamp = str(date - timedelta(days=1))
                for header in RELEVANT_HEADERS:
                    result[tid][header][str(date)] = result[tid][header].get(previous_timestamp, True)

    with open(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}-{aggregation_function.__name__}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


DATE = datetime(2023, 5, 1, 12, tzinfo=utc)
ARCHIVE_TABLE_NAME = f"archive_data_20230501"


def compute_archive_snapshot_stability(targets: List[Tuple[int, str, str]],
                                       start: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN'),
                                       end: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MAX')) -> None:
    """Compute the stability of archived snapshots from `start` date up to (inclusive) `end` date."""
    assert start <= end

    archive_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp::date, end_url, headers->>%s, status_code 
            FROM {ARCHIVE_TABLE_NAME}
            WHERE (headers->>%s IS NOT NULL OR status_code=404) AND timestamp::date BETWEEN %s AND %s
        """, (MEMENTO_HEADER.lower(), MEMENTO_HEADER.lower(), start, end))
        for tid, date, end_url, memento_datetime, status_code in cursor.fetchall():
            if memento_datetime is not None:
                memento_datetime = datetime.strptime(memento_datetime, MEMENTO_HEADER_FORMAT).replace(tzinfo=utc)
            archive_data[tid, date] = (end_url, memento_datetime, status_code)

    result = defaultdict(dict)
    for tid, _, _ in tqdm(targets):
        previous_status = Status.MISSING
        previous_snapshot = None
        for date in date_range(start, end):
            if (tid, date) not in archive_data:
                if previous_status in (Status.ADDED, Status.MODIFIED):
                    status = Status.UNMODIFIED
                elif previous_status == Status.REMOVED:
                    status = Status.MISSING
                else:
                    status = previous_status
            else:
                end_url, memento_datetime, status_code = archive_data[tid, date]
                if memento_datetime is None:
                    assert status_code == 404
                    if previous_status in (Status.ADDED, Status.MODIFIED, Status.UNMODIFIED):
                        status = Status.REMOVED
                    else:
                        status = Status.MISSING
                elif abs(memento_datetime - DATE) > timedelta(1):
                    if previous_status in (Status.ADDED, Status.MODIFIED):
                        status = Status.UNMODIFIED
                    elif previous_status == Status.REMOVED:
                        status = Status.MISSING
                    else:
                        status = previous_status
                else:
                    if previous_status in (Status.MISSING, Status.REMOVED):
                        status = Status.ADDED
                    else:
                        status = Status.MODIFIED if previous_snapshot != end_url else Status.UNMODIFIED

                    previous_snapshot = end_url

            result[tid][str(date)] = status
            previous_status = status

    with open(join_with_json_path(f"STABILITY-{ARCHIVE_TABLE_NAME}-snapshots-{start}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    # LIVE DATA
    analyze_live_data(get_tranco_data(), aggregation_function=normalize_headers)

    analyze_live_data(get_tranco_data(), aggregation_function=classify_headers)

    # ARCHIVE DATA
    compute_archive_snapshot_stability(
        get_tranco_data()
    )

    compute_archive_snapshot_stability(
        get_tranco_data(),
        start=get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN') + timedelta(days=1)
    )


if __name__ == '__main__':
    main()
