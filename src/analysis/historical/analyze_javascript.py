import json
from collections import defaultdict, Counter
from datetime import timedelta
from pathlib import Path

from tqdm import tqdm

from analysis.analysis_utils import parse_site
from analysis.header_utils import HeadersDecoder
from analysis.post_processing.extract_script_metadata import METADATA_TABLE_NAME
from configs.analysis import MEMENTO_HEADER
from configs.crawling import TIMESTAMPS
from configs.database import get_database_cursor
from configs.utils import join_with_json_path, get_tranco_data, compute_tolerance_window
from data_collection.collect_archive_data import TABLE_NAME as ARCHIVE_TABLE_NAME


def analyze_inclusions() -> None:
    """Analyze the number of script inclusions for all fresh hits per timestamp."""
    inclusions = {}
    with get_database_cursor() as cursor:
        for timestamp in tqdm(TIMESTAMPS):
            cursor.execute(f"""
                SELECT ARRAY_AGG(urls_count), ARRAY_AGG(hosts_count), ARRAY_AGG(sites_count) FROM (
                    SELECT JSONB_ARRAY_LENGTH(relevant_sources) AS urls_count, 
                           JSONB_ARRAY_LENGTH(hosts) AS hosts_count,
                           JSONB_ARRAY_LENGTH(sites) AS sites_count
                    FROM {ARCHIVE_TABLE_NAME} JOIN {METADATA_TABLE_NAME} USING (content_hash)
                    WHERE timestamp=%s AND (headers->>%s)::TIMESTAMPTZ BETWEEN %s AND %s
                ) AS INCLUSIONS
            """, (timestamp, MEMENTO_HEADER.lower(), *compute_tolerance_window(timestamp, timedelta(weeks=6))))

            relevant_sources_count, hosts_count, sites_count = cursor.fetchone()
            inclusions[str(timestamp)] = {
                'scripts': relevant_sources_count,
                'hosts': hosts_count,
                'sites': sites_count
            }

    with open(join_with_json_path(f"JAVASCRIPT-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(inclusions, file, indent=2, sort_keys=True)


def analyze_inclusion_bounds(urls: list[tuple[int, str, str]], neighborhoods_path: Path) -> None:
    """Analyze the number of script inclusions per neighborhood by computing the union and intersection of sources."""
    with open(neighborhoods_path) as file:
        neighborhoods = json.load(file, cls=HeadersDecoder)

    result = defaultdict(lambda: defaultdict(dict))
    counts = defaultdict(lambda: defaultdict(Counter))
    for tid, _, _ in tqdm(urls):
        for timestamp in TIMESTAMPS:
            if len(neighborhoods[str(tid)][str(timestamp)]) < 2:
                continue

            scripts = defaultdict(list)
            for *_, relevant_sources, hosts, sites, _, _ in neighborhoods[str(tid)][str(timestamp)]:
                scripts['scripts'].append(set(relevant_sources))
                scripts['hosts'].append(set(hosts))
                scripts['sites'].append(set(sites))

            for granularity in 'scripts', 'hosts', 'sites':
                result[tid][granularity][str(timestamp)] = {
                    'Union': len(set.union(*scripts[granularity])) if scripts[granularity] else 0,
                    'Intersection': len(set.intersection(*scripts[granularity])) if scripts[granularity] else 0
                }

                for script in set.union(*scripts[granularity]):
                    counts[granularity][str(timestamp)][script] += 1

    with open(join_with_json_path(f"JAVASCRIPT-{neighborhoods_path.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)

    with open(join_with_json_path(f"JAVASCRIPT-COUNTS-{neighborhoods_path.name}"), 'w') as file:
        json.dump(counts, file, indent=2, sort_keys=True)


def analyze_trackers(urls: list[tuple[int, str, str]], neighborhoods_path: Path) -> None:
    """Analyze the number of injected trackers inclusions per neighborhood."""
    with open(neighborhoods_path) as file:
        neighborhoods = json.load(file, cls=HeadersDecoder)

    result = defaultdict(dict)
    counts = {'trackers': defaultdict(Counter)}
    for tid, _, _ in tqdm(urls):
        for timestamp in TIMESTAMPS:
            if len(neighborhoods[str(tid)][str(timestamp)]) < 2:
                continue

            trackers = []
            for *_, disconnect_trackers, easyprivacy_trackers in neighborhoods[str(tid)][str(timestamp)]:
                trackers.append(set(map(parse_site, disconnect_trackers)) | set(map(parse_site, easyprivacy_trackers)))

            result[tid][str(timestamp)] = {
                'Union': sorted(set.union(*trackers)) if trackers else [],
                'Intersection': sorted(set.intersection(*trackers)) if trackers else []
            }

            for tracker in set.union(*trackers):
                counts['trackers'][str(timestamp)][tracker] += 1

    with open(join_with_json_path(f"TRACKERS-{neighborhoods_path.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)

    with open(join_with_json_path(f"TRACKERS-COUNTS-{neighborhoods_path.name}"), 'w') as file:
        json.dump(counts, file, indent=2, sort_keys=True)


def main():
    analyze_inclusions()
    analyze_inclusion_bounds(get_tranco_data(), join_with_json_path(f"NEIGHBORHOODS.{10}.json"))
    analyze_trackers(get_tranco_data(), join_with_json_path(f"NEIGHBORHOODS.{10}.json"))


if __name__ == '__main__':
    main()
