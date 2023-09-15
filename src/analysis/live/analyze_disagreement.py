import json
from collections import defaultdict
from datetime import datetime, timedelta, UTC
from pathlib import Path

from tqdm import tqdm

from analysis.analysis_utils import parse_archived_headers, is_tracker
from analysis.header_utils import Headers, parse_origin, normalize_headers, classify_headers
from analysis.post_processing.extract_script_metadata import METADATA_TABLE_NAME
from configs.analysis import RELEVANT_HEADERS, INTERNET_ARCHIVE_END_URL_REGEX, MEMENTO_HEADER, \
    SECURITY_MECHANISM_HEADERS
from configs.crawling import ARCHIVE_IT_USER_AGENT
from configs.database import get_database_cursor
from configs.utils import get_tranco_data, join_with_json_path
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from data_collection.crawling import crawl, CrawlingException

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

    for mechanism in SECURITY_MECHANISM_HEADERS:
        if normalized_archived_headers[mechanism] != normalized_live_headers[mechanism]:
            result[f"SYNTAX_DIFFERENCE_{mechanism}"].add(url)
            result['SYNTAX_DIFFERENCE'].add(url)
            if classified_archived_headers[mechanism] != classified_live_headers[mechanism]:
                result[f"SEMANTICS_DIFFERENCE_{mechanism}"].add(url)
                result['SEMANTICS_DIFFERENCE'].add(url)

    result['DIFFERENT' if url in result['SYNTAX_DIFFERENCE'] else 'EQUAL'].add(url)

    return result


def merge_analysis_results(destination: defaultdict[str, set[str]], source: dict[str, set[str]]) -> None:
    """Merge `source` dict into `destination` dict by joining their items."""
    for key in source:
        destination[key] |= source[key]


def analyze_headers(targets: list[tuple[int, str, str]]) -> None:
    """Analyze the provided `urls` by comparing the headers of the corresponding live and archive data."""
    analysis_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT l.start_url, l.end_url, l.status_code, l.headers,
                   (a.headers->>%s)::TIMESTAMPTZ, substring(a.end_url FROM %s), a.status_code, a.headers
            FROM {LIVE_TABLE_NAME} l JOIN {ARCHIVE_TABLE_NAME} a USING (tranco_id, status_code)
            WHERE l.status_code IS NOT NULL AND a.headers->>%s IS NOT NULL 
            AND l.timestamp::date=%s AND a.timestamp::date=%s
        """, (MEMENTO_HEADER.lower(), INTERNET_ARCHIVE_END_URL_REGEX,
              MEMENTO_HEADER.lower(), TIMESTAMP.date(), TIMESTAMP.date()))
        for start_url, *data, archive_headers in cursor.fetchall():
            analysis_data[start_url] = (*data, parse_archived_headers(archive_headers))

    result = defaultdict(set)
    for tid, domain, url in tqdm(targets):
        if url not in analysis_data:
            result['FAIL'].add(url)
            continue

        (live_end_url, live_status_code, live_headers,
         memento_datetime, archived_end_url, archived_status_code, archived_headers) = analysis_data[url]

        if abs(memento_datetime - TIMESTAMP) > timedelta(1):
            result['OUTDATED'].add(url)
            continue

        result['SUCCESS'].add(url)

        for header in RELEVANT_HEADERS:
            if archived_headers.get(header) or live_headers.get(header):
                result[f"USES_{header}"].add(url)
                result['USES_ANY'].add(url)

        header_comparison_result = compare_security_headers(url, live_headers, archived_headers)
        merge_analysis_results(result, header_comparison_result)

        # check if inconsistency (if there is any) is due to different origin
        origin_mismatch = parse_origin(live_end_url) != parse_origin(archived_end_url)
        for granularity in 'SYNTAX_DIFFERENCE', 'SEMANTICS_DIFFERENCE':
            for mechanism in SECURITY_MECHANISM_HEADERS:
                category = f"{granularity}_{mechanism}"
                if url in result[category]:
                    if origin_mismatch:
                        result[f"{category}::ORIGIN_MISMATCH"].add(url)
                        result[f"{granularity}::ORIGIN_MISMATCH"].add(url)

    raw_output_path = join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json")
    with open(raw_output_path, 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True, default=list)

    result = {key: len(value) for key, value in result.items()}
    with open(join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def analyze_user_agent_sniffing(disagreement_file: Path) -> None:
    """Check if header value inconsistency is due to user agent sniffing."""
    with open(disagreement_file) as file:
        disagreement = json.load(file)

    result = defaultdict(set)
    for url in tqdm(disagreement['DIFFERENT']):
        try:
            chrome = crawl(url, store_content=False)
        except CrawlingException:
            result['ERROR_CHROME'].add(url)

        try:
            archive_org = crawl(url, user_agent=ARCHIVE_IT_USER_AGENT, store_content=False)
        except CrawlingException:
            result['ERROR_ARCHIVE_BOT'].add(url)

        if url in result['ERROR_CHROME'] or url in result['ERROR_ARCHIVE_BOT']:
            if url in result['ERROR_CHROME'] and url in result['ERROR_ARCHIVE_BOT']:
                result['ERROR_BOTH'].add(url)
            continue

        normalized_chrome = normalize_headers(chrome.headers)
        normalized_archive_org = normalize_headers(archive_org.headers)
        classified_chrome = classify_headers(chrome.headers, parse_origin(chrome.url))
        classified_archive_org = classify_headers(archive_org.headers, parse_origin(archive_org.url))

        for mechanism in SECURITY_MECHANISM_HEADERS:
            if normalized_chrome[mechanism] != normalized_archive_org[mechanism]:
                result[f"SYNTAX_DIFFERENCE_{mechanism}"].add(url)
                result['SYNTAX_DIFFERENCE'].add(url)
                if classified_chrome[mechanism] != classified_archive_org[mechanism]:
                    result[f"SEMANTICS_DIFFERENCE_{mechanism}"].add(url)
                    result['SEMANTICS_DIFFERENCE'].add(url)

        result['DIFFERENT' if url in result['SYNTAX_DIFFERENCE'] else 'EQUAL'].add(url)

    raw_output_path = join_with_json_path(f"DISAGREEMENT-UA-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json")
    with open(raw_output_path, 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True, default=list)

    result = {key: len(value) for key, value in result.items()}
    with open(join_with_json_path(f"DISAGREEMENT-UA-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def merge_disagreement_reasons(disagreement_file: Path, user_agent_sniffing_file: Path) -> None:
    with open(disagreement_file) as file:
        disagreement = defaultdict(set, json.load(file))

    with open(user_agent_sniffing_file) as file:
        user_agent_sniffing = defaultdict(set, json.load(file))

    for granularity in 'SYNTAX_DIFFERENCE', 'SEMANTICS_DIFFERENCE':
        disagreement[f"{granularity}::USER_AGENT"] = user_agent_sniffing[f"{granularity}"]
        disagreement[f"{granularity}::NO_INFORMATION"] = (
                set(disagreement[f"{granularity}"])
                - set(disagreement[f"{granularity}::ORIGIN_MISMATCH"])
                - set(disagreement[f"{granularity}::USER_AGENT"])
        )
        for mechanism in SECURITY_MECHANISM_HEADERS:
            disagreement[f"{granularity}_{mechanism}::USER_AGENT"] = user_agent_sniffing[f"{granularity}_{mechanism}"]
            disagreement[f"{granularity}_{mechanism}::NO_INFORMATION"] = (
                    set(disagreement[f"{granularity}_{mechanism}"])
                    - set(disagreement[f"{granularity}_{mechanism}::ORIGIN_MISMATCH"])
                    - set(disagreement[f"{granularity}_{mechanism}::USER_AGENT"])
            )

    raw_output_path = join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json")
    with open(raw_output_path, 'w') as file:
        json.dump(disagreement, file, indent=2, sort_keys=True, default=list)

    disagreement = {key: len(value) for key, value in disagreement.items()}
    with open(join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(disagreement, file, indent=2, sort_keys=True)


def analyze_trackers(targets: list[tuple[int, str, str]]) -> None:
    """Analyze the provided `urls` by comparing the included scripts of the corresponding live and archive data."""
    analysis_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT l.start_url, l.end_url, l.status_code, 
                   sl.relevant_sources, sl.hosts, sl.sites,
                   (a.headers->>%s)::TIMESTAMPTZ, substring(a.end_url FROM %s), a.status_code, 
                   sa.relevant_sources, sa.hosts, sa.sites
            FROM {LIVE_TABLE_NAME} l JOIN {ARCHIVE_TABLE_NAME} a USING (tranco_id, status_code)
            JOIN {METADATA_TABLE_NAME} sl ON l.content_hash=sl.content_hash
            JOIN {METADATA_TABLE_NAME} sa ON a.content_hash=sa.content_hash
            WHERE l.status_code IS NOT NULL AND a.headers->>%s IS NOT NULL AND l.timestamp::date=%s AND a.timestamp::date=%s
        """, (MEMENTO_HEADER.lower(), INTERNET_ARCHIVE_END_URL_REGEX,
              MEMENTO_HEADER.lower(), TIMESTAMP.date(), TIMESTAMP.date()))
        for start_url, *data in cursor.fetchall():
            analysis_data[start_url] = tuple(data)

    result = defaultdict(set)
    for tid, domain, url in tqdm(targets):
        if url not in analysis_data:
            result['FAIL'].add(url)
            continue

        (live_end_url, live_status_code, live_scripts, live_hosts, live_sites, memento_datetime, archived_end_url,
         archived_status_code, archive_scripts, archived_hosts, archived_sites) = analysis_data[url]

        if parse_origin(live_end_url) != parse_origin(archived_end_url):
            result['ORIGIN_MISMATCH'].add(url)
            continue

        if abs(memento_datetime - TIMESTAMP) > timedelta(1):
            result['OUTDATED'].add(url)
            continue

        result['SUCCESS'].add(url)

        if set(live_scripts) | set(archive_scripts):
            result['INCLUDES_SCRIPTS'].add(url)

            if set(live_scripts) != set(archive_scripts):
                result['DIFFERENT_URLS'].add(url)
            if set(live_hosts) != set(archived_hosts):
                result['DIFFERENT_HOSTS'].add(url)
            if set(live_sites) != set(archived_sites):
                result['DIFFERENT_SITES'].add(url)

            if bool(live_scripts) ^ bool(archive_scripts):
                result['SCRIPTS_INCLUSION_EITHER_MISSING'].add(url)

        live_trackers = {script for script in live_scripts if is_tracker(script, parse_origin(live_end_url))}
        archived_trackers = {script for script in archive_scripts if is_tracker(script, parse_origin(archived_end_url))}

        if live_trackers | archived_trackers:
            result['INCLUDES_TRACKERS'].add(url)

            if live_trackers != archived_trackers:
                result['DIFFERENT_TRACKERS'].add(url)

            if bool(live_trackers) ^ bool(archived_trackers):
                result['TRACKER_INCLUSION_EITHER_MISSING'].add(url)

    with open(join_with_json_path(f"DISAGREEMENT-JS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True, default=list)

    result = {key: len(value) for key, value in result.items()}
    with open(join_with_json_path(f"DISAGREEMENT-JS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    analyze_headers(get_tranco_data())
    analyze_user_agent_sniffing(
        join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json")
    )
    merge_disagreement_reasons(
        join_with_json_path(f"DISAGREEMENT-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json"),
        join_with_json_path(f"DISAGREEMENT-UA-HEADERS-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.RAW.json")
    )

    analyze_trackers(get_tranco_data())


if __name__ == '__main__':
    main()
