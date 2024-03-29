import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable

from tqdm import tqdm

from analysis.analysis_utils import timedelta_to_days, parse_site
from analysis.header_utils import Headers, Origin, parse_origin, normalize_headers, classify_headers
from analysis.live.stability_enums import Status
from analysis.post_processing.extract_script_metadata import METADATA_TABLE_NAME
from configs.analysis import RELEVANT_HEADERS, MEMENTO_HEADER
from configs.database import get_database_cursor, get_min_timestamp, get_max_timestamp
from configs.utils import join_with_json_path, get_tranco_data, date_range
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME


def analyze_live_headers(targets: list[tuple[int, str, str]],
                         start: datetime = get_min_timestamp(LIVE_TABLE_NAME),
                         end: datetime = get_max_timestamp(LIVE_TABLE_NAME),
                         aggregation_function: Callable[[Headers, Origin | None], Headers] = normalize_headers) -> None:
    """Compute the stability of (crawled) live security headers from `start` up to (inclusive) `end`."""
    assert start <= end

    live_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp, headers, end_url
            FROM {LIVE_TABLE_NAME}
            WHERE status_code=200 AND timestamp BETWEEN %s AND %s
        """, (start, end))
        for tid, timestamp, headers, end_url in cursor.fetchall():
            live_data[tid, timestamp] = (headers, aggregation_function(headers, parse_origin(end_url)))

    result = {tid: {header: {'DEPLOYS': False} for header in RELEVANT_HEADERS} for tid, _, _ in targets}
    for tid, _, _ in tqdm(targets):
        seen_values = defaultdict(set)
        for timestamp in date_range(start, end):
            if (tid, timestamp) in live_data:
                headers, aggregated_headers = live_data[tid, timestamp]
                for header in RELEVANT_HEADERS:
                    seen_values[header].add(aggregated_headers[header])
                    result[tid][header][str(timestamp)] = len(seen_values[header]) == 1
                    result[tid][header]['DEPLOYS'] |= header in headers
            else:
                previous_timestamp = str(timestamp - timedelta(days=1))
                for header in RELEVANT_HEADERS:
                    result[tid][header][str(timestamp)] = result[tid][header].get(previous_timestamp, True)

    with open(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}.{aggregation_function.__name__}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def analyze_live_js_inclusions(targets: list[tuple[int, str, str]],
                               start: datetime = get_min_timestamp(LIVE_TABLE_NAME),
                               end: datetime = get_max_timestamp(LIVE_TABLE_NAME)) -> None:
    """Compute the stability of (crawled) live security headers from `start` date up to (inclusive) `end` date."""
    assert start <= end

    live_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp,
                   relevant_sources, hosts, sites, disconnect_trackers, easyprivacy_trackers, end_url
            FROM {LIVE_TABLE_NAME} JOIN {METADATA_TABLE_NAME} USING (content_hash)
            WHERE status_code=200 AND timestamp BETWEEN %s AND %s
        """, (start, end))
        for tid, timestamp, *data, end_url in cursor.fetchall():
            live_data[tid, timestamp] = (*map(tuple, data), parse_origin(end_url))

    result = defaultdict(lambda: defaultdict(dict))
    for tid, _, _ in tqdm(targets):
        seen_values = defaultdict(set)
        includes_scripts = False
        includes_trackers = False
        for timestamp in date_range(start, end):
            if (tid, timestamp) in live_data:
                relevant_sources, hosts, sites, disconnect, easyprivacy, origin = live_data[tid, timestamp]
                includes_scripts |= len(relevant_sources) > 0
                seen_values['scripts'].add(relevant_sources)
                result[tid]['scripts'][str(timestamp)] = len(seen_values['scripts']) == 1
                seen_values['hosts'].add(hosts)
                result[tid]['hosts'][str(timestamp)] = len(seen_values['hosts']) == 1
                seen_values['sites'].add(sites)
                result[tid]['sites'][str(timestamp)] = len(seen_values['sites']) == 1

                trackers = tuple(sorted(set(map(parse_site, disconnect)) | set(map(parse_site, easyprivacy))))
                includes_trackers |= len(trackers) > 0
                seen_values['trackers'].add(trackers)
                result[tid]['trackers'][str(timestamp)] = len(seen_values['trackers']) == 1
            else:
                previous_timestamp = str(timestamp - timedelta(days=1))
                result[tid]['scripts'][str(timestamp)] = result[tid]['scripts'].get(previous_timestamp, True)
                result[tid]['hosts'][str(timestamp)] = result[tid]['hosts'].get(previous_timestamp, True)
                result[tid]['sites'][str(timestamp)] = result[tid]['sites'].get(previous_timestamp, True)
                result[tid]['trackers'][str(timestamp)] = result[tid]['trackers'].get(previous_timestamp, True)
        result[tid]['INCLUDES_SCRIPTS'] = includes_scripts
        result[tid]['INCLUDES_TRACKERS'] = includes_trackers

    with open(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}.JS.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


ARCHIVE_TABLE_NAME = 'HISTORICAL_DATA_20230716_20230730'


def analyze_archived_snapshots(targets: list[tuple[int, str, str]],
                               start: datetime = get_min_timestamp(ARCHIVE_TABLE_NAME),
                               end: datetime = get_max_timestamp(ARCHIVE_TABLE_NAME),
                               n: int = 10) -> None:
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
            for i, date in enumerate(date_range(requested_date.date(), requested_date.date() + timedelta(days=n))):
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

    with open(join_with_json_path(f"STABILITY-{ARCHIVE_TABLE_NAME}.snapshots.json"), 'w') as file:
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
