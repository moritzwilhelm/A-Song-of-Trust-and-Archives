import json
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.ticker import PercentFormatter
from pandas import DataFrame

from analysis.analysis_utils import get_aggregated_date
from analysis.live.stability_enums import Status
from configs.analysis import RELEVANT_HEADERS
from configs.utils import join_with_json_path, json_to_plots_path, date_range
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from plotting.plotting_utils import HEADER_ABBREVIATION, STYLE, COLORS

ARCHIVE_TABLE_NAME = 'archive_data_20230501'


def plot_live(input_path: Path,
              start: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MIN'),
              end: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MAX')) -> None:
    """Plot the analyzed live data in `input_path` between `start` and `end` and save the figure at `output_path`."""
    assert start <= end

    with open(input_path) as file:
        results = json.load(file)

    data = DataFrame()

    for header in RELEVANT_HEADERS:
        total = sum(results[tid][header]['DEPLOYS'] for tid in results)
        data[HEADER_ABBREVIATION[header]] = [
            sum(results[tid][header][str(date)] for tid in results if results[tid][header]['DEPLOYS']) / total
            for date in date_range(start, end)
        ]

    axes = data.plot(style=STYLE, color=COLORS, grid=True)
    axes.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    axes.set_xlabel('Days')
    axes.set_ylabel('Stable sites')

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def plot_snapshot_stability(input_path: Path,
                            start: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN'),
                            end: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MAX')) -> None:
    """Plot the analyzed archive data in `input_path` between `start` and `end` and save the figure at `output_path`."""
    assert start <= end

    with open(input_path) as file:
        results = json.load(file)

    data = DataFrame()

    dates = list(date_range(start, end))
    data['Additions'] = [sum(results[tid][str(date)] == Status.ADDED for tid in results) for date in dates]
    data['Updates'] = [sum(results[tid][str(date)] == Status.MODIFIED for tid in results) for date in dates]
    data['Deletions'] = [sum(results[tid][str(date)] == Status.REMOVED for tid in results) for date in dates]
    data['Hits'] = data['Additions'].cumsum() - data['Deletions']

    axes = data[['Hits']].plot(style=STYLE, color=COLORS)
    axes = data[['Deletions', 'Updates']].plot.bar(color=COLORS[1:], grid=True, ax=axes, rot=0)
    axes.set_xlabel('Days')
    axes.set_ylabel('Affected Sites')

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def main():
    # LIVE DATA
    plot_live(
        join_with_json_path('STABILITY-LIVE_DATA-normalize_headers.json')
    )

    plot_live(
        join_with_json_path('STABILITY-LIVE_DATA-classify_headers.json')
    )

    # ARCHIVE DATA
    start_date = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN')
    plot_snapshot_stability(
        join_with_json_path(f"STABILITY-archive_data_20230501-snapshots-{start_date}.json")
    )

    plot_snapshot_stability(
        join_with_json_path(f"STABILITY-archive_data_20230501-snapshots-{start_date + timedelta(days=1)}.json"),
        start=start_date + timedelta(days=1)
    )


if __name__ == '__main__':
    main()
