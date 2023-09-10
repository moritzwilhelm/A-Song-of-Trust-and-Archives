import json
from collections import defaultdict
from datetime import timedelta

from tqdm import tqdm

from analysis.analysis_utils import timedelta_to_days
from configs.analysis import MEMENTO_HEADER
from configs.crawling import TIMESTAMPS
from configs.database import get_database_cursor
from configs.files.random_sample_tranco import RANDOM_SAMPLING_TABLE_NAME
from configs.utils import join_with_json_path, compute_tolerance_window
from data_collection.collect_archive_data import TABLE_NAME


def compute_hits(table_name: str, tolerance: timedelta | None = None) -> None:
    """Compute the number of archive hits that match the given `tolerance`."""
    num_hits = {}
    with get_database_cursor() as cursor:
        for timestamp in tqdm(TIMESTAMPS):
            cursor.execute(f"""
                SELECT count(DISTINCT tranco_id)
                FROM {table_name}
                WHERE timestamp=%s AND (headers->>%s)::TIMESTAMPTZ BETWEEN %s AND %s
            """, (timestamp, MEMENTO_HEADER.lower(), *compute_tolerance_window(timestamp, tolerance)))

            num_hits[str(timestamp)] = cursor.fetchone()[0]

    if tolerance is not None:
        tolerance = tolerance.days

    with open(join_with_json_path(f"COVERAGE-{table_name}.{tolerance}.json"), 'w') as file:
        json.dump(num_hits, file, indent=2, sort_keys=True)


def compute_drifts(table_name: str, tolerance: timedelta | None = None) -> None:
    """Collect the drifts between archived date and requested date, only considering hits that match the `tolerance`."""
    drifts = defaultdict(list)
    with get_database_cursor() as cursor:
        for timestamp in tqdm(TIMESTAMPS):
            cursor.execute(f"""
                SELECT (headers->>%s)::TIMESTAMPTZ
                FROM {table_name}
                WHERE timestamp=%s AND (headers->>%s)::TIMESTAMPTZ BETWEEN %s AND %s
            """, (MEMENTO_HEADER.lower(), timestamp, MEMENTO_HEADER.lower(),
                  *compute_tolerance_window(timestamp, tolerance)))

            for archived_date, in cursor.fetchall():
                drifts[str(timestamp)].append(timedelta_to_days(archived_date - timestamp))

    if tolerance is not None:
        tolerance = tolerance.days

    with open(join_with_json_path(f"DRIFTS-{table_name}.{tolerance}.json"), 'w') as file:
        json.dump(drifts, file, indent=2, sort_keys=True)


def compute_hits_per_bucket(tolerance: timedelta | None = None) -> None:
    """Compute the number of archive hits per 100k bucket that match the given `tolerance`."""
    num_hits = defaultdict(dict)
    with get_database_cursor() as cursor:
        for start, end in tqdm([(i, i + 99_999) for i in range(1, 1_000_000, 100_000)]):
            for timestamp in TIMESTAMPS:
                cursor.execute(f"""
                    SELECT count(DISTINCT tranco_id)
                    FROM {RANDOM_SAMPLING_TABLE_NAME}
                    WHERE timestamp=%s AND (headers->>%s)::TIMESTAMPTZ BETWEEN %s AND %s AND tranco_id BETWEEN %s AND %s
                """, (timestamp, MEMENTO_HEADER.lower(), *compute_tolerance_window(timestamp, tolerance), start, end))

                num_hits[str(timestamp)][end // 1_000] = cursor.fetchone()[0]

    if tolerance is not None:
        tolerance = tolerance.days

    with open(join_with_json_path(f"BUCKETS-{RANDOM_SAMPLING_TABLE_NAME}.{tolerance}.json"), 'w') as file:
        json.dump(num_hits, file, indent=2, sort_keys=True)


def main():
    for table_name in TABLE_NAME, RANDOM_SAMPLING_TABLE_NAME:
        compute_hits(table_name)
        compute_hits(table_name, tolerance=timedelta(weeks=6))

        compute_drifts(table_name)
        compute_drifts(table_name, tolerance=timedelta(weeks=6))

    compute_hits_per_bucket()
    compute_hits_per_bucket(tolerance=timedelta(weeks=6))


if __name__ == '__main__':
    main()