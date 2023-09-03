from collections import defaultdict
from datetime import datetime, timedelta, UTC
from pprint import pprint

from tqdm import tqdm

from analysis.analysis_utils import parse_archived_headers
from analysis.header_utils import parse_origin, normalize_headers, classify_headers, Headers
from configs.analysis import RELEVANT_HEADERS, INTERNET_ARCHIVE_END_URL_REGEX, MEMENTO_HEADER
from configs.database import get_database_cursor
from configs.utils import get_tranco_data
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME

TIMESTAMP = datetime(2023, 9, 1, 12, tzinfo=UTC)
ARCHIVE_TABLE_NAME = 'HISTORICAL_DATA_FOR_COMPARISON'


def compare_security_headers(url: str, live_headers: Headers, archived_headers: Headers) -> dict[str, set[str]]:
    """Compare live and archived headers based on their security level."""
    result = defaultdict(set)
    origin = parse_origin(url)

    normalized_live_headers = normalize_headers(live_headers)
    normalized_archived_headers = normalize_headers(archived_headers)

    classified_live_headers = classify_headers(live_headers, origin)
    classified_archived_headers = classify_headers(archived_headers, origin)

    for header in RELEVANT_HEADERS:
        if normalized_archived_headers[header] != normalized_live_headers[header]:
            result[f"SYNTAX_DIFFERENCE_{header}"].add(url)
            result['SYNTAX_DIFFERENCE'].add(url)
            if classified_archived_headers[header] != classified_live_headers[header]:
                result[f"SEMANTICS_DIFFERENCE_{header}"].add(url)
                result['SEMANTICS_DIFFERENCE'].add(url)

    result['DIFFERENT' if url in result['SYNTAX_DIFFERENCE'] else 'EQUAL'].add(url)

    return result


def merge_analysis_results(destination: defaultdict[str, set[str]], source: dict[str, set[str]]) -> None:
    """Merge `source` dict into `destination` dict by joining their items."""
    for key in source:
        destination[key] |= source[key]


def analyze(targets: list[tuple[int, str, str]]) -> None:
    """Analyze the provided `urls` by comparing corresponding live and archive data."""
    analysis_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT l.start_url, l.end_url, l.status_code, l.headers,
                   (a.headers->>%s)::TIMESTAMPTZ, substring(a.end_url FROM %s), a.status_code, a.headers
            FROM {LIVE_TABLE_NAME} l JOIN {ARCHIVE_TABLE_NAME} a USING (domain, status_code)
            WHERE l.status_code IS NOT NULL AND a.->>%s IS NOT NULL AND l.timestamp::date=%s AND a.timestamp::date=%s
        """, (MEMENTO_HEADER.lower(), INTERNET_ARCHIVE_END_URL_REGEX,
              MEMENTO_HEADER.lower(), TIMESTAMP.date(), TIMESTAMP.date()))
        for start_url, *data, archive_headers in cursor.fetchall():
            analysis_data[start_url] = (*data, parse_archived_headers(archive_headers))

    results = defaultdict(set)
    for tid, domain, url in tqdm(targets):
        if url not in analysis_data:
            results['FAIL'].add(url)
            continue

        (live_end_url, live_status_code, live_headers,
         memento_datetime, archived_end_url, archived_status_code, archived_headers) = analysis_data[url]

        if abs(memento_datetime - TIMESTAMP) > timedelta(1):
            results['OUTDATED'].add(url)
            continue

        results['SUCCESS'].add(url)

        if live_status_code != archived_status_code:
            results['DIFFERENT_STATUS_CODE'].add(url)

        if live_end_url != archived_end_url:
            results['DIFFERENT_END_URL'].add(url)

        if parse_origin(live_end_url) != parse_origin(archived_end_url):
            results['DIFFERENT_END_URL_ORIGIN'].add(url)

        for header in RELEVANT_HEADERS:
            if archived_headers.get(header) or live_headers.get(header):
                results[f"USES_{header}"].add(url)
                results['USES_ANY'].add(url)

        merge_analysis_results(results, compare_security_headers(url, live_headers, archived_headers))

    results = {key: len(value) for key, value in results.items()}
    pprint(results)


def main():
    analyze(get_tranco_data())


if __name__ == '__main__':
    main()
