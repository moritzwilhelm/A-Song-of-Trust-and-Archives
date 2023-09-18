import json
from collections import defaultdict, Counter
from datetime import datetime, timedelta, UTC
from pathlib import Path

from tqdm import tqdm

from analysis.analysis_utils import parse_archived_headers
from analysis.header_utils import HeadersEncoder, HeadersDecoder
from analysis.post_processing.extract_script_metadata import METADATA_TABLE_NAME as SCRIPTS_TABLE_NAME
from configs.analysis import INTERNET_ARCHIVE_END_URL_REGEX, MEMENTO_HEADER, INTERNET_ARCHIVE_SOURCE_HEADER
from configs.crawling import INTERNET_ARCHIVE_TIMESTAMP_FORMAT, TIMESTAMPS
from configs.database import get_database_cursor
from configs.utils import join_with_json_path, get_tranco_data
from data_collection.collect_archive_neighborhoods import CANDIDATES_TABLE_NAME, TABLE_NAME as NEIGHBORHOODS_TABLE_NAME
from data_collection.collect_contributors import METADATA_TABLE_NAME as CONTRIBUTORS_TABLE_NAME


def get_neighbors(n: int = 10):
    """Retrieve all neighborhood members per (tranco_id, timestamp) neighborhood from the database."""
    neighbors = defaultdict(list)
    with (get_database_cursor() as cursor):
        cursor.execute(f"""
            SELECT tranco_id, timestamp, candidates
            FROM {CANDIDATES_TABLE_NAME}
            WHERE error IS NULL
        """)
        for tid, timestamp, candidates in cursor.fetchall():
            neighbors[tid, timestamp] = \
                [datetime.strptime(ts, INTERNET_ARCHIVE_TIMESTAMP_FORMAT).replace(tzinfo=UTC) for ts in candidates[:n]]

    return neighbors


def build_neighborhoods(targets: list[tuple[int, str, str]], n: int = 10) -> None:
    """Build all (tranco_id, timestamp) neighborhoods of size `n` for all `targets`."""
    neighbors = get_neighbors(n)
    archive_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp,
                   (headers->>%s)::TIMESTAMPTZ, headers, substring(end_url FROM %s), status_code, contributor,
                   relevant_sources, hosts, sites, disconnect_trackers, easyprivacy_trackers
            FROM {NEIGHBORHOODS_TABLE_NAME}
            JOIN {SCRIPTS_TABLE_NAME} USING (content_hash)
            JOIN {CONTRIBUTORS_TABLE_NAME} ON SPLIT_PART(headers->>%s, '/', 1)=source
            WHERE (headers->>%s)::TIMESTAMPTZ IS NOT NULL
        """, (MEMENTO_HEADER.lower(), INTERNET_ARCHIVE_END_URL_REGEX, INTERNET_ARCHIVE_SOURCE_HEADER.lower(),
              MEMENTO_HEADER.lower()))
        for tid, timestamp, archived_timestamp, headers, *data in cursor.fetchall():
            archive_data[tid, timestamp] = (archived_timestamp, parse_archived_headers(headers), *data)

    def get_neighborhood(tranco_id: int, ts: datetime) -> list[tuple]:
        """Build the neighborhood for (tranco_id, ts) and ignore duplicate members."""
        neighborhood = []
        seen_timestamps = set()
        for neighbor_timestamp in neighbors[tranco_id, ts]:
            if (tranco_id, neighbor_timestamp) in archive_data:
                archived_timestamp, *data = archive_data[tranco_id, neighbor_timestamp]
                if abs(ts - archived_timestamp) <= timedelta(weeks=6) and archived_timestamp not in seen_timestamps:
                    neighborhood.append((str(archived_timestamp), *data))
                    seen_timestamps.add(archived_timestamp)
        return neighborhood

    neighborhoods = defaultdict(dict)
    for tid, _, _ in tqdm(targets):
        for timestamp in TIMESTAMPS:
            neighborhoods[tid][str(timestamp)] = get_neighborhood(tid, timestamp)

    with open(join_with_json_path(f"NEIGHBORHOODS.{n}.json"), 'w') as file:
        json.dump(neighborhoods, file, indent=2, sort_keys=True, cls=HeadersEncoder)


def analyze_neighborhood_sizes(neighborhoods_path: Path):
    """Compute the size of each neighborhood."""
    with open(neighborhoods_path) as file:
        data = json.load(file, cls=HeadersDecoder)

    result = defaultdict(lambda: defaultdict(list))
    for tid in tqdm(data):
        for timestamp, neighborhood in data[tid].items():
            result[timestamp]['size'].append(len(neighborhood))

    with open(neighborhoods_path.with_name(f"SIZES-{neighborhoods_path.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def analyze_contributors(neighborhood_path: Path):
    """Compute the number of snapshots per contributor, considering all neighborhoods of size >= 2."""
    with open(neighborhood_path) as file:
        data = json.load(file, cls=HeadersDecoder)

    result = Counter()
    for tid in tqdm(data):
        for timestamp, neighborhood in data[tid].items():
            if len(neighborhood) >= 2:
                for _, _, _, _, contributor, *_ in neighborhood:
                    result[contributor] += 1

    with open(neighborhood_path.with_name(f"CONTRIBUTORS-{neighborhood_path.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    build_neighborhoods(get_tranco_data())
    analyze_neighborhood_sizes(join_with_json_path(f"NEIGHBORHOODS.{10}.json"))
    analyze_contributors(join_with_json_path(f"NEIGHBORHOODS.{10}.json"))


if __name__ == '__main__':
    main()
