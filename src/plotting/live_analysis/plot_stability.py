import json
from datetime import datetime, UTC
from datetime import timedelta
from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.ticker import PercentFormatter
from pandas import DataFrame, read_json

from analysis.live.analyze_stability import ARCHIVE_TABLE_NAME
from analysis.live.stability_enums import Status
from configs.analysis import RELEVANT_HEADERS
from configs.database import get_min_timestamp, get_max_timestamp
from configs.utils import join_with_json_path, json_to_plots_path, date_range
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from plotting.plotting_utils import HEADER_ABBREVIATION, STYLE, COLORS, latexify


@latexify(xtick_minor_visible=True)
def plot_headers_stability(input_path: Path,
                           start: datetime = get_min_timestamp(LIVE_TABLE_NAME),
                           end: datetime = get_min_timestamp(LIVE_TABLE_NAME) + timedelta(30)) -> None:
    """Plot the stability of security header values of live data in `input_path` between `start` and `end`."""
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
    axes.xaxis.get_minor_ticks()[0].set_visible(False)
    axes.xaxis.get_minor_ticks()[-1].set_visible(False)
    axes.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    axes.set_xlabel('Passed days')
    axes.set_ylabel('Stable domains')

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify(xtick_minor_visible=True)
def plot_js_stability(input_path: Path,
                      start: datetime = get_min_timestamp(LIVE_TABLE_NAME),
                      end: datetime = get_min_timestamp(LIVE_TABLE_NAME) + timedelta(30)) -> None:
    """Plot the stability of JS inclusions of live data in `input_path` between `start` and `end`."""
    assert start <= end

    with open(input_path) as file:
        results = json.load(file)

    df = DataFrame()

    for column in 'scripts', 'hosts', 'sites':
        total = sum(results[tid]['INCLUDES_SCRIPTS'] for tid in results)
        df[column] = [
            sum(results[tid][column][str(date)] for tid in results if results[tid]['INCLUDES_SCRIPTS']) / total
            for date in date_range(start, end)
        ]

    total_including_trackers = sum(results[tid]['INCLUDES_TRACKERS'] for tid in results)
    df['trackers'] = [
        sum(results[tid]['trackers'][str(date)] for tid in results if
            results[tid]['INCLUDES_TRACKERS']) / total_including_trackers
        for date in date_range(start, end)
    ]

    axes = df.plot(style=STYLE, color=COLORS, grid=True)
    axes.xaxis.get_minor_ticks()[0].set_visible(False)
    axes.xaxis.get_minor_ticks()[-1].set_visible(False)
    axes.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    axes.set_xlabel('Passed days')
    axes.set_ylabel('Stable domains')

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify()
def plot_snapshot_stability(input_path: Path,
                            start: datetime = get_min_timestamp(ARCHIVE_TABLE_NAME),
                            end: datetime = get_max_timestamp(ARCHIVE_TABLE_NAME),
                            n: int = 11) -> None:
    """Plot the stability of archived snapshots in `input_path` between `start` and `end`."""
    assert start <= end

    with open(input_path) as file:
        json_data = json.load(file)

    data = DataFrame()
    data['Insertions'] = [[] for _ in range(n)]
    data['Updates'] = [[] for _ in range(n)]
    data['Deletions'] = [[] for _ in range(n)]
    data['Fresh Hits'] = [[] for _ in range(n)]

    for current_timestamp in date_range(start, end):
        results = json_data[str(current_timestamp)]
        dates = list(date_range(current_timestamp.date(), current_timestamp.date() + timedelta(n - 1)))

        current = DataFrame()
        current['Insertions'] = [sum(results[tid][str(date)] == Status.ADDED for tid in results) for date in dates]
        current['Updates'] = [sum(results[tid][str(date)] == Status.MODIFIED for tid in results) for date in dates]
        current['Deletions'] = [sum(results[tid][str(date)] == Status.REMOVED for tid in results) for date in dates]
        current['Fresh Hits'] = current['Insertions'].cumsum() - current['Deletions'].cumsum()

        axes = current[['Fresh Hits']].plot(style=STYLE, color=COLORS)
        axes = current[['Deletions', 'Updates']].plot.bar(color=COLORS[1:], grid=True, ax=axes, rot=0)
        axes.set_xlabel('Passed days')
        axes.set_ylabel('Affected domains')
        axes.set_yticks(range(0, 11_001, 1000))
        axes.set_ylim(top=11_000)
        # axes.set_title(current_timestamp.date())
        axes.legend(loc='lower right')

        axes.figure.savefig(json_to_plots_path(input_path.with_suffix(f".{current_timestamp.date()}.json")),
                            bbox_inches='tight', dpi=300)

        axes.figure.show()
        plt.close()

        for column in data.columns:
            for idx, row in current.iterrows():
                data.at[idx, column].append(row[column])

    if start == end:
        return

    for column in data.columns:
        axes = data.explode(column).reset_index().pivot(columns='index', values=column).astype(float).plot.box(
            grid=True,
            boxprops=dict(linestyle='-', linewidth=1, color=COLORS[0]),
            whiskerprops=dict(linestyle='dotted', linewidth=1, color=COLORS[1]),
            medianprops=dict(linestyle='-', linewidth=1, color=COLORS[2]),
            capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
            flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4]),
            # showfliers=False
        )
        axes.set_xlabel('Passed days')
        axes.set_ylabel(column)

        axes.figure.savefig(json_to_plots_path(input_path.with_suffix(f".{column}.json")), bbox_inches='tight', dpi=300)

        axes.figure.show()
        plt.close()


def plot_drifts(input_path: Path) -> None:
    """Plot the temporal drifts between archived date and requested date."""
    with open(input_path) as file:
        data = read_json(file, orient='index').transpose()

    axes = data.plot.box(
        grid=True,
        boxprops=dict(linestyle='-', linewidth=1, color=COLORS[0]),
        whiskerprops=dict(linestyle='dotted', linewidth=1, color=COLORS[1]),
        medianprops=dict(linestyle='-', linewidth=1, color=COLORS[2]),
        capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
        flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4]),
        showfliers=False
    )
    axes.set_xlabel('Passed days')
    axes.set_ylabel('Temporal drift in days')

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify()
def main():
    # LIVE DATA
    plot_headers_stability(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}.normalize_headers.json"))
    plot_headers_stability(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}.classify_headers.json"))

    plot_js_stability(join_with_json_path(f"STABILITY-{LIVE_TABLE_NAME}.JS.json"))

    # ARCHIVE DATA
    plot_snapshot_stability(join_with_json_path(f"STABILITY-{ARCHIVE_TABLE_NAME}.snapshots.json"))

    plot_snapshot_stability(
        join_with_json_path('STABILITY-archive_data_20230501.snapshots.json'),
        start=datetime(2023, 5, 1, 12, tzinfo=UTC),
        end=datetime(2023, 5, 1, 12, tzinfo=UTC)
    )

    plot_drifts(join_with_json_path(f"REMOVE-DRIFTS-{ARCHIVE_TABLE_NAME}.json"))


if __name__ == '__main__':
    main()
