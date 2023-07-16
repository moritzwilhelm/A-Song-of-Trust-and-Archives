import json
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter

from analysis.analysis_utils import get_aggregated_date
from analysis.live.stability_enums import Status
from configs.analysis import RELEVANT_HEADERS
from configs.utils import join_with_json_path, join_with_plots_path, date_range
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from plotting.plotting_utils import HEADER_ABBREVIATION, STYLE

DATE = "20230501"
ARCHIVE_TABLE_NAME = f"archive_data_{DATE}"


def plot_live(input_path: Path,
              output_path: Path,
              start: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MIN'),
              end: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MAX')) -> None:
    """Plot the analyzed live data in `input_path` between `start` and `end` and save the figure at `output_path`."""
    assert start <= end

    with open(input_path) as file:
        results = json.load(file)

    for line_id, header in enumerate(RELEVANT_HEADERS):
        total = sum(results[tid][header]['DEPLOYS'] for tid in results)
        data = [
            sum(results[tid][header][str(date)] for tid in results if results[tid][header]['DEPLOYS']) / total
            for date in date_range(start, end)]
        plt.plot(data, ls=STYLE[line_id][1:], marker=STYLE[line_id][0], label=HEADER_ABBREVIATION[header])

    plt.grid(axis='both')
    plt.legend(loc='lower right')
    plt.xlabel('Days')
    plt.ylabel('Stable sites')
    num_days = (end - start).days + 1
    plt.xticks(range(0, num_days, 1))
    plt.xlim(-1, num_days + 3)
    plt.gca().yaxis.set_major_formatter(PercentFormatter(xmax=1))

    plt.savefig(output_path, bbox_inches='tight', dpi=300)

    plt.show()
    plt.clf()


def plot_snapshot_stability(input_path: Path,
                            output_path: Path,
                            start: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN'),
                            end: date_type = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MAX')) -> None:
    """Plot the analyzed archive data in `input_path` between `start` and `end` and save the figure at `output_path`."""
    assert start <= end

    with open(input_path) as file:
        results = json.load(file)

    palette = sns.color_palette('colorblind')

    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    num_days = (end - start).days + 1
    dates = [start + timedelta(days=i) for i in range(num_days)]

    def get_status_list(tid, date):
        return {results[tid][str(day)] for day in date_range(start, date)}

    additions = [sum(results[tid][str(date)] == Status.ADDED for tid in results) for date in dates]
    print('ADDITIONS', additions)
    snapshots = [sum(Status.ADDED in get_status_list(tid, date) for tid in results) for date in dates]
    print('SNAPSHOTS', snapshots)
    modifications = [sum(results[tid][str(date)] == Status.MODIFIED for tid in results) for date in dates]
    print('MODIFICATIONS', modifications)
    deletions = [sum(results[tid][str(date)] == Status.REMOVED for tid in results) for date in dates]
    print('DELETIONS', deletions)

    ax.plot(snapshots, label='Sites with snapshot', color=palette[0])  # , width=0.2)
    ax2.bar([x + 0.2 for x in range(0, num_days)], deletions, label='Deletions', width=0.2, color=palette[1])
    ax2.bar([x for x in range(0, num_days)], modifications, label='Updates', width=0.2, color=palette[2])

    ax2.grid(axis='both', color=palette[2], alpha=.5)
    # ax2.grid(axis='y', color=palette[0])
    # plt.legend()
    # ax2.legend()
    ax.legend(loc='upper right')
    ax2.legend(loc='lower right')
    plt.xlabel('Days')
    # plt.ylim(0, 1800)
    ax.set_ylabel('Total sites with working snapshots')
    # right labels
    ax.yaxis.label.set_color(palette[0])
    ax2.spines['left'].set_color(palette[0])
    ax2.spines['right'].set_color(palette[2])
    ax.tick_params(axis='y', colors=palette[0])
    ax.set_ylim(0, 12000)

    # left labels
    ax2.yaxis.label.set_color(palette[2])
    ax2.tick_params(axis='y', colors=palette[2])
    ax2.set_ylim(0, 7000)

    # ax2.yaxis.ticks.set_color(palette[4])
    ax2.set_ylabel('Sites with Updates/Deletions')
    plt.xticks(range(0, num_days, 1))

    plt.savefig(output_path, bbox_inches='tight', dpi=300)

    plt.show()
    plt.clf()


def main():
    # LIVE DATA
    plot_live(
        join_with_json_path('STABILITY-LIVE_DATA-normalize_headers.json'),
        join_with_plots_path('STABILITY-LIVE_DATA-normalize_headers.pdf')
    )

    plot_live(
        join_with_json_path('STABILITY-LIVE_DATA-classify_headers.json'),
        join_with_plots_path('STABILITY-LIVE_DATA-classify_headers.pdf')
    )

    # ARCHIVE DATA
    start_date = get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN')
    plot_snapshot_stability(
        join_with_json_path(f"STABILITY-archive_data_20230501-snapshots-{start_date}.json"),
        join_with_plots_path(f"STABILITY-archive_data_20230501-snapshots-{start_date}.pdf")
    )

    plot_snapshot_stability(
        join_with_json_path(f"STABILITY-archive_data_20230501-snapshots-{start_date + timedelta(days=1)}.json"),
        join_with_plots_path(f"STABILITY-archive_data_20230501-snapshots-{start_date + timedelta(days=1)}.pdf"),
        start=start_date + timedelta(days=1)
    )


if __name__ == '__main__':
    main()
