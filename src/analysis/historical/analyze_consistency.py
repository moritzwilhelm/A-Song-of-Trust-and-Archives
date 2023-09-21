import json
from collections import defaultdict
from pathlib import Path
from typing import Callable

from tqdm import tqdm

from analysis.header_utils import Headers, HeadersDecoder, Origin, parse_origin, normalize_headers, classify_headers
from configs.analysis import SECURITY_MECHANISM_HEADERS
from configs.crawling import TIMESTAMPS
from configs.utils import join_with_json_path, get_tranco_data


def analyze_consistency(urls: list[tuple[int, str, str]],
                        neighborhoods_path: Path,
                        aggregation_function: Callable[[Headers, Origin | None], Headers] = normalize_headers) -> None:
    """Compute the consistency of header values within each neighborhood."""
    with open(neighborhoods_path) as file:
        neighborhoods = json.load(file, cls=HeadersDecoder)

    result = defaultdict(lambda: defaultdict(dict))
    for tid, _, _ in tqdm(urls):
        for timestamp in TIMESTAMPS:
            seen_values = defaultdict(set)
            deploys = defaultdict(lambda: False)
            for _, headers, end_url, *_ in neighborhoods[str(tid)][str(timestamp)]:
                aggregated_headers = aggregation_function(headers, parse_origin(end_url))
                for security_mechanism, header in SECURITY_MECHANISM_HEADERS.items():
                    seen_values[security_mechanism].add(aggregated_headers[security_mechanism])
                    deploys[header] |= header in headers

            for security_mechanism, header in SECURITY_MECHANISM_HEADERS.items():
                result[tid][security_mechanism][str(timestamp)] = (
                    deploys[header],
                    len(seen_values[security_mechanism]),
                    len(neighborhoods[str(tid)][str(timestamp)]),
                    any(header not in headers for _, headers, *_ in neighborhoods[str(tid)][str(timestamp)])
                )

    output_path_name = f"CONSISTENCY-{neighborhoods_path.with_suffix(f'.{aggregation_function.__name__}.json').name}"
    with open(join_with_json_path(output_path_name), 'w') as file:
        json.dump(result, file, indent=2, sort_keys=True)


def main():
    for aggregation_function in normalize_headers, classify_headers:
        analyze_consistency(
            get_tranco_data(),
            join_with_json_path(f"NEIGHBORHOODS.{10}.json"),
            aggregation_function=aggregation_function
        )


if __name__ == '__main__':
    main()
