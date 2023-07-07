from itertools import islice
from pathlib import Path
from typing import List, Tuple

from configs.crawling import NUMBER_URLS, PREFIX

PROJECT_ROOT = Path('<AUTOMATICALLY-REPLACED-DURING-INSTALL>')
INSTALLED_PROJECT_ROOT = Path(__file__).parents[1].resolve()


def join_with_json_path(filename: str) -> Path:
    """Return absolute path to the specified `filename` in the results/json directory."""
    return PROJECT_ROOT.joinpath('results', 'json', filename)


def join_with_plots_path(filename: str) -> Path:
    """Return absolute path to the specified `filename` in the results/json directory."""
    return PROJECT_ROOT.joinpath('results', 'plots', filename)


def get_absolute_tranco_file_path() -> Path:
    """Return absolute path to the Tranco file."""
    return PROJECT_ROOT.joinpath('configs', 'tranco_W9JG9.csv')


def get_tranco_data(tranco_file: Path = get_absolute_tranco_file_path(),
                    n: int = NUMBER_URLS) -> List[Tuple[int, str, str]]:
    """Read `n` domains from the given `tranco_file` and expand them into full urls by prepending `PREFIX`."""
    data = []
    with open(tranco_file) as file:
        for line in islice(file, n):
            tranco_id, domain = line.strip().split(',')
            data.append((int(tranco_id), domain, f"{PREFIX}{domain}"))
    return data
