import json
from collections import defaultdict

from analysis.live.analyze_disagreement import TIMESTAMP, ARCHIVE_TABLE_NAME
from configs.analysis import MEMENTO_HEADER
from configs.database import get_database_cursor
from configs.utils import join_with_json_path
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME

FACTOR = 1_000_000


def analyze_overhead() -> None:
    """Analyze the request overhead of the Internet Archive."""
    result = defaultdict(list)
    with get_database_cursor() as cursor:
        cursor.execute(f"""
            SELECT l.response_time / %s, a.response_time / %s
            FROM {LIVE_TABLE_NAME} l JOIN {ARCHIVE_TABLE_NAME} a USING (tranco_id, status_code)
            WHERE l.status_code IS NOT NULL AND a.headers->>%s IS NOT NULL AND l.timestamp::date=%s AND a.timestamp::date=%s
        """, (FACTOR, FACTOR, MEMENTO_HEADER.lower(), TIMESTAMP.date(), TIMESTAMP.date()))
        for live_response_time, archive_response_time in cursor.fetchall():
            result['Live Measurement'].append(float(live_response_time))
            result['Archive-based Measurement'].append(float(archive_response_time))
            result['Internet Archive Overhead'].append(float(archive_response_time - live_response_time))

    if True:
        for key in result:
            print(key, len(result[key]), sum(result[key])/len(result[key]))
        print(min(result['Live Measurement']), max(result['Live Measurement']))
        print(min(result['Archive-based Measurement']), max(result['Archive-based Measurement']))
        print(min(result['Internet Archive Overhead']), max(result['Internet Archive Overhead']))

    with open(join_with_json_path(f"OVERHEAD-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"), 'w') as file:
        json.dump(result, file, indent=2)


def main():
    analyze_overhead()


if __name__ == '__main__':
    main()
