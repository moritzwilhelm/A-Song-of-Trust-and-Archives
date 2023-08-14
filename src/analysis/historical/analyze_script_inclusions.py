import json
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

from analysis.extract_script_metadata import METADATA_TABLE_NAME
from analysis.header_utils import HeadersDecoder
from configs.crawling import TIMESTAMPS
from configs.database import get_database_cursor
from configs.utils import join_with_json_path, get_tranco_data
from data_collection.collect_archive_data import TABLE_NAME


# TODO only consider real neighborhoods, aka use first member of neighborhood instead of all(?) or just keep it as it is
def analyze_inclusions():
    inclusions = {}
    with get_database_cursor() as cursor:
        for timestamp in tqdm(TIMESTAMPS):
            cursor.execute(f"""
                SELECT SUM(rs_count), SUM(ho_count), SUM(si_count) FROM (
                    SELECT JSONB_ARRAY_LENGTH(relevant_sources) AS rs_count, JSONB_ARRAY_LENGTH(hosts) AS ho_count,
                           JSONB_ARRAY_LENGTH(sites) AS si_count
                    FROM {TABLE_NAME} JOIN {METADATA_TABLE_NAME} USING (content_hash)
                    WHERE timestamp=%s
                ) AS INCLUSIONS
            """, (timestamp,))

            relevant_sources_count, hosts_count, sites_count = cursor.fetchone()
            inclusions[str(timestamp)] = {
                'relevant_sources': relevant_sources_count,
                'hosts': hosts_count,
                'sites': sites_count
            }

    with open(join_with_json_path(f"SCRIPT-INCLUSIONS-{TABLE_NAME}.json"), 'w') as file:
        json.dump(inclusions, file, indent=2, sort_keys=True)


def analyze_inclusion_bounds(urls: list[tuple[int, str, str]], proximity_sets_path: Path) -> None:
    """Compute the stability of (crawled) live data from `start` date up to (inclusive) `end` date."""
    with open(proximity_sets_path) as file:
        proximity_sets = json.load(file, cls=HeadersDecoder)

    result = defaultdict(lambda: defaultdict(dict))
    for tid, _, _ in tqdm(urls):
        for timestamp in TIMESTAMPS:
            scripts = defaultdict(list)

            for _, _, relevant_sources, hosts, sites in proximity_sets[str(tid)][str(timestamp)]:
                scripts['relevant_sources'].append(set(relevant_sources))
                scripts['hosts'].append(set(hosts))
                scripts['sites'].append(set(sites))

            for granularity in 'relevant_sources', 'hosts', 'sites':
                result[tid][granularity][str(timestamp)] = {
                    'UNION': len(set.union(*scripts[granularity])) if scripts[granularity] else 0,
                    'INTERSECTION': len(set.intersection(*scripts[granularity])) if scripts[granularity] else 0
                }

    with open(join_with_json_path(f"SCRIPT-INCLUSIONS-{proximity_sets_path.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def analyze_trackers():
    pass


def main():
    analyze_inclusions()
    analyze_inclusion_bounds(get_tranco_data(), join_with_json_path(f"PROXIMITY-SETS-{10}.json"), )


if __name__ == '__main__':
    main()
