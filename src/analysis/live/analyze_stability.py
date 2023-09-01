import json
from collections import defaultdict
from datetime import datetime, date as date_type, timedelta
from typing import Callable

from tqdm import tqdm

from analysis.analysis_utils import timedelta_to_days
from analysis.header_utils import Headers, Origin, parse_origin, normalize_headers, classify_headers
from analysis.live.stability_enums import Status
from analysis.post_processing.extract_script_metadata import METADATA_TABLE_NAME
from configs.analysis import RELEVANT_HEADERS, MEMENTO_HEADER
from configs.database import get_database_cursor, get_min_timestamp, get_max_timestamp
from configs.utils import join_with_json_path, get_tranco_data, date_range, get_tracking_domains
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def analyze_live_headers(targets: list[tuple[int, str, str]],
                         start: date_type = get_min_timestamp(LIVE_TABLE_NAME).date(),
                         end: date_type = get_max_timestamp(LIVE_TABLE_NAME).date(),
                         aggregation_function: Callable[[Headers, Origin | None], Headers] = normalize_headers) -> None:
    """Compute the stability of (crawled) live security headers from `start` date up to (inclusive) `end` date."""
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


def analyze_live_js_inclusions(targets: list[tuple[int, str, str]],
                               start: date_type = get_min_timestamp(LIVE_TABLE_NAME).date(),
                               end: date_type = get_max_timestamp(LIVE_TABLE_NAME).date()) -> None:
    """Compute the stability of (crawled) live security headers from `start` date up to (inclusive) `end` date."""
    assert start <= end

    live_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp::date, relevant_sources, hosts, sites
            FROM {LIVE_TABLE_NAME} JOIN {METADATA_TABLE_NAME} USING (content_hash)
            WHERE status_code=200 AND timestamp::date BETWEEN %s AND %s
        """, (start, end))
        for tid, date, *data in cursor.fetchall():
            live_data[tid, date] = data

    tracking_domains = get_tracking_domains()

    result = defaultdict(lambda: defaultdict(dict))
    for tid, _, _ in tqdm(targets):
        seen_values = defaultdict(set)
        for date in date_range(start, end):
            if (tid, date) in live_data:
                relevant_sources, hosts, sites = live_data[tid, date]
                trackers = (set(host for host in hosts if host in tracking_domains) |
                            set(site for site in sites if site in tracking_domains))
                seen_values['urls'].add(tuple(relevant_sources))
                result[tid]['urls'][str(date)] = len(seen_values['urls']) == 1
                seen_values['hosts'].add(tuple(hosts))
                result[tid]['hosts'][str(date)] = len(seen_values['hosts']) == 1
                seen_values['sites'].add(tuple(sites))
                result[tid]['sites'][str(date)] = len(seen_values['sites']) == 1
                seen_values['trackers'].add(tuple(trackers))
                result[tid]['trackers'][str(date)] = len(seen_values['trackers']) == 1
                seen_values['uses-trackers'].add(len(trackers) == 0)
                result[tid]['uses-trackers'][str(date)] = len(seen_values['uses-trackers']) == 1
            else:
                previous_timestamp = str(date - timedelta(days=1))
                result[tid]['urls'][str(date)] = result[tid]['urls'].get(previous_timestamp, True)
                result[tid]['hosts'][str(date)] = result[tid]['hosts'].get(previous_timestamp, True)
                result[tid]['sites'][str(date)] = result[tid]['sites'].get(previous_timestamp, True)
                result[tid]['urls'][str(date)] = result[tid]['urls'].get(previous_timestamp, True)
                result[tid]['trackers'][str(date)] = result[tid]['trackers'].get(previous_timestamp, True)
                result[tid]['uses-trackers'][str(date)] = result[tid]['uses-trackers'].get(previous_timestamp, True)

    with open(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}-TRACKING.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


ARCHIVE_TABLE_NAME = 'HISTORICAL_DATA_20230716_20230730'


def analyze_archived_snapshots(targets: list[tuple[int, str, str]],
                               start: datetime = get_min_timestamp(ARCHIVE_TABLE_NAME),
                               end: datetime = get_max_timestamp(ARCHIVE_TABLE_NAME),
                               n: int = 14) -> None:
    """Compute the stability of archived snapshots from `start` date up to (inclusive) `end` date."""
    assert start <= end

    drifts = defaultdict(list)
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
            was_removed = False
            current_drifts = {}
            for i, date in enumerate(date_range(requested_date.date(), requested_date.date() + timedelta(n))):
                if (tid, date) not in archive_data:
                    match previous_status:
                        case Status.ADDED | Status.MODIFIED:
                            status = Status.UNMODIFIED
                        case Status.REMOVED:
                            status = Status.MISSING
                        case _:
                            status = previous_status
                else:
                    end_url, memento_datetime, status_code = archive_data[tid, date]

                    # compute drifts
                    if memento_datetime is not None:
                        current_drifts[i] = timedelta_to_days(memento_datetime - requested_date)

                    if memento_datetime is None or abs(memento_datetime - requested_date) > timedelta(1):
                        match previous_status:
                            case Status.ADDED | Status.MODIFIED | Status.UNMODIFIED:
                                status = Status.REMOVED
                                was_removed = True
                            case _:
                                status = Status.MISSING
                    else:
                        match previous_status:
                            case Status.MISSING | Status.REMOVED:
                                status = Status.ADDED
                            case _:
                                status = Status.MODIFIED if previous_snapshot != end_url else Status.UNMODIFIED

                        previous_snapshot = end_url

                result[str(requested_date)][tid][str(date)] = status
                previous_status = status

            if was_removed:
                for i in current_drifts:
                    drifts[i].append(current_drifts[i])

    with open(join_with_json_path(f"REMOVE-DRIFTS-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(drifts, file, indent=2, sort_keys=True)

    with open(join_with_json_path(f"STABILITY-{ARCHIVE_TABLE_NAME}-snapshots.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    # LIVE DATA
    analyze_live_headers(get_tranco_data(), aggregation_function=normalize_headers)

    analyze_live_headers(get_tranco_data(), aggregation_function=classify_headers)

    analyze_live_js_inclusions(get_tranco_data())

    # ARCHIVE DATA
    analyze_archived_snapshots(
        get_tranco_data()
    )


if __name__ == '__main__':
    main()
