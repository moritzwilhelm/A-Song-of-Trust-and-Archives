import json
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

from analysis.analysis_utils import get_aggregated_date
from analysis.live.stability_enums import Status
from configs.analysis import RELEVANT_HEADERS
from configs.utils import join_with_json_path, join_with_plots_path
from data_collection.collect_archive_data import TABLE_NAME as ARCHIVE_TABLE_NAME
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from plotting.plotting_utils import HEADER_ABBREVIATION, STYLE


def plot_live(input_path: Path,
              output_path: Path,
              start: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MIN'),
              end: date_type = get_aggregated_date(LIVE_TABLE_NAME, 'MAX')) -> None:
    """Plot the analyzed live data in `input_path` between `start` and `end` and save the figure at `output_path`."""
    assert start <= end

    num_days = (end - start).days + 1
    line_id = 0
    with open(input_path) as f:
        results = json.load(f)

    for h in RELEVANT_HEADERS:
        total = sum(results[url][f"USES-{h}"] for url in results)
        data = [
            sum(results[url][f"STABLE-{h}"][str(date)] for url in results if results[url][f"USES-{h}"]) / total
            for date in (start + timedelta(days=i) for i in range(num_days))]
        plt.plot(data, ls=STYLE[line_id][1:], marker=STYLE[line_id][0], label=HEADER_ABBREVIATION[h])
        line_id += 1

    plt.grid(axis='both')
    plt.legend(loc='lower right')
    plt.xlabel('Days')
    plt.ylabel('Stable sites')
    plt.xticks(range(0, num_days, 1))
    plt.xlim(-1, num_days + 3)
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))
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

    def get_status_list(url, date):
        return {results[url][str(day)] for day in (start + timedelta(days=j) for j in range((date - start).days + 1))}

    additions = [sum(results[url][str(date)] == Status.ADDED.value for url in results) for date in dates]
    print('ADDITIONS', additions)
    snapshots = [sum(Status.ADDED.value in get_status_list(url, date) for url in results) for date in dates]
    print('SNAPSHOTS', snapshots)
    modifications = [sum(results[url][str(date)] == Status.MODIFIED.value for url in results) for date in dates]
    print('MODIFICATIONS', modifications)
    deletions = [sum(results[url][str(date)] == Status.REMOVED.value for url in results) for date in dates]
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


if __name__ == '__main__':
    # LIVE DATA
    plot_live(
        join_with_json_path('STABILITY-live_data-normalize_headers.json'),
        join_with_plots_path('STABILITY-live_data-normalize_headers.pdf')
    )
    plot_live(
        join_with_json_path('STABILITY-live_data-classify_headers.json'),
        join_with_plots_path('STABILITY-live_data-classify_headers.pdf')
    )

    # ARCHIVE DATA
    plot_snapshot_stability(
        join_with_json_path('STABILITY-archive_data_20230501-snapshots.json'),
        join_with_plots_path('STABILITY-archive_data_20230501-snapshots.pdf')
    )

    # plot_snapshot_stability(
    #    join_with_json_path('STABILITY-archive_data_20230501-snapshots.json'),
    #    join_with_plots_path('STABILITY-archive_data_20230501-snapshots.pdf'),
    #    start=get_aggregated_date(ARCHIVE_TABLE_NAME, 'MIN') + timedelta(days=1)
    # )
