import json
from collections import defaultdict
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
from data_collection.collect_archive_proximity_sets import CANDIDATES_TABLE_NAME, \
    TABLE_NAME as PROXIMITY_SETS_TABLE_NAME
from data_collection.collect_contributors import METADATA_TABLE_NAME as CONTRIBUTORS_TABLE_NAME


def get_proximity_set_members(n: int = 10):
    """Retrieve all proximity set members per (tranco_id, timestamp) proximity set from the database."""
    result = defaultdict(list)
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp, candidates
            FROM {CANDIDATES_TABLE_NAME}
            WHERE error IS NULL
        """)
        for tid, timestamp, candidates in cursor.fetchall():
            result[tid, timestamp] = [datetime.strptime(ts, INTERNET_ARCHIVE_TIMESTAMP_FORMAT).replace(tzinfo=UTC)
                                      for ts in candidates[:n]]

    return result


def build_proximity_sets(targets: list[tuple[int, str, str]], n: int = 10) -> None:
    """Build all (tranco_id, timestamp) proximity sets of size `n` for all `targets`."""
    proximity_set_members = get_proximity_set_members(n)
    ps_members_data = {}
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT tranco_id, timestamp, (headers->>%s)::TIMESTAMPTZ,
                   headers, substring(end_url FROM %s), status_code, contributor, relevant_sources, hosts, sites
            FROM {PROXIMITY_SETS_TABLE_NAME}
            JOIN {SCRIPTS_TABLE_NAME} USING (content_hash)
            JOIN {CONTRIBUTORS_TABLE_NAME} ON SPLIT_PART(headers->>%s, '/', 1)=source
            WHERE (headers->>%s)::TIMESTAMPTZ IS NOT NULL
        """, (MEMENTO_HEADER.lower(), INTERNET_ARCHIVE_END_URL_REGEX, INTERNET_ARCHIVE_SOURCE_HEADER.lower(),
              MEMENTO_HEADER.lower()))
        for tid, timestamp, archived_timestamp, headers, *data in cursor.fetchall():
            ps_members_data[tid, timestamp] = (archived_timestamp, parse_archived_headers(headers), *data)

    def get_proximity_set(tranco_id: int, ts: datetime) -> list[tuple]:
        """Build the proximity set for (tranco_id, ts) and drop duplicate members."""
        proximity_set = []
        seen_timestamps = set()
        for member_timestamp in proximity_set_members[tranco_id, ts]:
            if (tranco_id, member_timestamp) in ps_members_data:
                archived_timestamp, *data = ps_members_data[tranco_id, member_timestamp]
                if abs(ts - archived_timestamp) <= timedelta(weeks=6) and archived_timestamp not in seen_timestamps:
                    proximity_set.append(tuple((str(archived_timestamp), *data)))
                    seen_timestamps.add(archived_timestamp)
        return proximity_set

    proximity_sets = defaultdict(dict)
    for tid, _, _ in tqdm(targets):
        for timestamp in TIMESTAMPS:
            proximity_sets[tid][str(timestamp)] = get_proximity_set(tid, timestamp)

    with open(join_with_json_path(f"PROXIMITY-SETS-{n}.json"), 'w') as file:
        json.dump(proximity_sets, file, indent=2, sort_keys=True, cls=HeadersEncoder)


def analyze_proximity_sets(proximity_sets_filepath: Path):
    """Compute the size of each proximity set and the number of distinct urls in each proximity set."""
    with open(proximity_sets_filepath) as file:
        data = json.load(file, cls=HeadersDecoder)

    result = defaultdict(lambda: defaultdict(list))
    for tid in tqdm(data):
        for timestamp, proximity_set in data[tid].items():
            result[timestamp]['Set size'].append(len(proximity_set))

    with open(proximity_sets_filepath.with_name(f"STATS-{proximity_sets_filepath.name}"), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    build_proximity_sets(get_tranco_data())
    analyze_proximity_sets(join_with_json_path(f"PROXIMITY-SETS-{10}.json"))


if __name__ == '__main__':
    main()
