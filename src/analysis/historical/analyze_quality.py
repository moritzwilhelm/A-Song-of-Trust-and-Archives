import json
from collections import defaultdict
from pathlib import Path
from typing import Callable

from tqdm import tqdm

from analysis.analysis_utils import parse_origin
from analysis.header_utils import Headers, HeadersDecoder, normalize_headers, classify_headers
from configs.analysis import RELEVANT_HEADERS
from configs.crawling import TIMESTAMPS
from configs.utils import join_with_json_path, get_tranco_data


def analyze_differences(urls: list[tuple[int, str, str]],
                        proximity_sets_path: Path,
                        aggregation_function: Callable[[Headers, str | None], Headers] = normalize_headers) -> None:
    """Compute the stability of (crawled) live data from `start` date up to (inclusive) `end` date."""
    with open(proximity_sets_path) as file:
        proximity_sets = json.load(file, cls=HeadersDecoder)

    result = defaultdict(lambda: defaultdict(dict))
    for tid, _, _ in tqdm(urls):
        for timestamp in TIMESTAMPS:
            seen_values = defaultdict(set)
            deploys = defaultdict(lambda: False)
            for headers, end_url in proximity_sets[str(tid)][str(timestamp)]:
                aggregated_headers = aggregation_function(headers, parse_origin(end_url))
                for header in RELEVANT_HEADERS:
                    seen_values[header].add(aggregated_headers[header])
                    deploys[header] |= header in headers

            for header in RELEVANT_HEADERS:
                result[tid][header][str(timestamp)] = (
                    deploys[header],
                    len(seen_values[header]),
                    len(proximity_sets[str(tid)][str(timestamp)])
                )

    output_path_name = f"QUALITY-{proximity_sets_path.with_suffix(f'.{aggregation_function.__name__}.json').name}"
    with open(join_with_json_path(output_path_name), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    for aggregation_function in normalize_headers, classify_headers:
        analyze_differences(get_tranco_data(),
                            join_with_json_path(f"PROXIMITY-SETS-{10}.json"),
                            aggregation_function=aggregation_function)


if __name__ == '__main__':
    main()
