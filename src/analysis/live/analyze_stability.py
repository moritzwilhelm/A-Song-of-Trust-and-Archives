import json
from collections import defaultdict
from datetime import datetime, date as date_type, timedelta
from typing import Callable

from tqdm import tqdm

from analysis.analysis_utils import parse_origin, get_aggregated_timestamp, get_aggregated_timestamp_date
from analysis.header_utils import Headers, normalize_headers, classify_headers
from analysis.live.stability_enums import Status
from configs.analysis import RELEVANT_HEADERS, MEMENTO_HEADER
from configs.database import get_database_cursor
from configs.utils import join_with_json_path, get_tranco_data, date_range
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def analyze_live_data(targets: list[tuple[int, str, str]],
                      start: date_type = get_aggregated_timestamp_date(LIVE_TABLE_NAME, 'MIN'),
                      end: date_type = get_aggregated_timestamp_date(LIVE_TABLE_NAME, 'MAX'),
                      aggregation_function: Callable[[Headers, str | None], Headers] = normalize_headers) -> None:
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


ARCHIVE_TABLE_NAME = 'HISTORICAL_DATA_20230716_20230730'


def analyze_archived_snapshots(targets: list[tuple[int, str, str]],
                               start: datetime = get_aggregated_timestamp(ARCHIVE_TABLE_NAME, 'MIN'),
                               end: datetime = get_aggregated_timestamp(ARCHIVE_TABLE_NAME, 'MAX'),
                               n: int = 14) -> None:
    """Compute the stability of archived snapshots from `start` date up to (inclusive) `end` date."""
    assert start <= end

    result = defaultdict(lambda: defaultdict(dict))
    for requested_date in date_range(start, end):
        archive_data = {}
        with get_database_cursor() as cursor:
            cursor.execute(f"""
                SELECT tranco_id, crawl_datetime::date, end_url, (headers->>%s)::TIMESTAMPTZ, status_code
                FROM {ARCHIVE_TABLE_NAME}
                WHERE timestamp=%s AND (headers->>%s IS NOT NULL OR status_code=404)
            """, (MEMENTO_HEADER.lower(), requested_date, MEMENTO_HEADER.lower()))
            for tid, crawl_date, *data in cursor.fetchall():
                archive_data[tid, crawl_date] = data

        for tid, _, _ in tqdm(targets):
            previous_status = Status.MISSING
            previous_snapshot = None
            for date in date_range(requested_date.date(), requested_date.date() + timedelta(n)):
                if (tid, date) not in archive_data:
                    if previous_status in (Status.ADDED, Status.MODIFIED):
                        status = Status.UNMODIFIED
                    elif previous_status == Status.REMOVED:
                        status = Status.MISSING
                    else:
                        status = previous_status
                else:
                    end_url, memento_datetime, status_code = archive_data[tid, date]
                    if memento_datetime is None or abs(memento_datetime - requested_date) > timedelta(1):
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

                result[str(requested_date)][tid][str(date)] = status
                previous_status = status

    with open(join_with_json_path(f"STABILITY-{ARCHIVE_TABLE_NAME}-snapshots-{start.date()}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    # LIVE DATA
    analyze_live_data(get_tranco_data(), aggregation_function=normalize_headers)

    analyze_live_data(get_tranco_data(), aggregation_function=classify_headers)

    # ARCHIVE DATA
    analyze_archived_snapshots(
        get_tranco_data()
    )


if __name__ == '__main__':
    main()
