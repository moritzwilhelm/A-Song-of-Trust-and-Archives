from pathlib import Path


def join_with_json_path(filename: str) -> Path:
    """Return absolute path to the specified `filename` in the results/json directory."""
    return Path(__file__).parents[1].resolve().joinpath('results', 'json', filename)


def join_with_plots_path(filename: str) -> Path:
    """Return absolute path to the specified `filename` in the results/json directory."""
    return Path(__file__).parents[1].resolve().joinpath('results', 'plots', filename)


def get_absolute_tranco_file_path() -> Path:
    """Return absolute path to the Tranco file."""
    return Path(__file__).parent.resolve().joinpath('tranco_W9JG9.csv')
