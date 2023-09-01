import json
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator, PercentFormatter
from pandas import DataFrame

from configs.crawling import TIMESTAMPS
from configs.utils import join_with_json_path, join_with_plots_path, json_to_plots_path
from data_collection.collect_archive_data import TABLE_NAME as ARCHIVE_TABLE_NAME
from plotting.plotting_utils import COLORS, latexify, get_year_ticks, STYLE


def plot_script_inclusions(input_path: Path):
    """Plot the average number of script inclusions for all fresh hits per timestamp."""
    with open(input_path) as file:
        data = pd.read_json(file, orient='index')

    for column in data.columns:
        data[column] = data.explode(column).reset_index().pivot(columns='index', values=column).mean(axis=0)

    axes = data.plot.bar(color=COLORS, grid=True)
    axes.set_xlabel('Timestamp')
    axes.set_xticks(*get_year_ticks(), rotation=0)
    axes.set_ylabel('Average number of inclusions')
    axes.yaxis.set_major_locator(MaxNLocator(integer=True, steps=[1], min_n_ticks=10))

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def plot_script_inclusion_bounds(input_path: Path):
    """Plot the number of script inclusions per proximity set via the union and intersection of sources."""
    with open(input_path) as file:
        results = json.load(file)

    for granularity in 'urls', 'hosts', 'sites':
        data = DataFrame()
        for agg in 'Union', 'Intersection':
            data[agg] = [
                [results[tid][granularity][str(ts)][agg] for tid in results if str(ts) in results[tid][granularity]]
                for ts in TIMESTAMPS
            ]
            data[agg] = data.explode(agg).reset_index().pivot(columns='index', values=agg).mean(axis=0)

        axes = data.plot(style=STYLE, color=COLORS, grid=True)
        if granularity != 'urls':
            axes.scatter(x=0, y=3.2, color='none')
            axes.scatter(x=0, y=2.38, color='none')

        axes.set_xlabel('Timestamp')
        axes.set_xticks(*get_year_ticks(), rotation=0)
        axes.xaxis.get_minor_ticks()[0].set_visible(False)
        axes.xaxis.get_minor_ticks()[-1].set_visible(False)
        axes.set_ylabel('Average number of inclusions')
        axes.legend(ncol=3, loc='upper center', bbox_to_anchor=(0.5, 1.1))
        axes.set_title(granularity.capitalize() + '\n\n')

        axes.figure.savefig(json_to_plots_path(input_path, f".{granularity}.png"), bbox_inches='tight', dpi=300)

        axes.figure.show()
        plt.close()


def plot_domains_without_trackers(input_path: Path) -> None:
    """Plot the number of script inclusions per proximity set via the union and intersection of sources."""
    with open(input_path) as file:
        results = json.load(file)

    data = DataFrame()
    for agg in 'Union', 'Intersection':
        data[agg] = [[results[tid][str(ts)][agg] for tid in results if str(ts) in results[tid]] for ts in TIMESTAMPS]
        pivot = data.explode(agg).reset_index().pivot(columns='index', values=agg)
        data[agg] = pivot.map(lambda elem: elem == []).sum() / pivot.count()

    axes = data.plot(style=STYLE, color=COLORS, grid=True)

    axes.set_xlabel('Timestamp')
    axes.set_xticks(*get_year_ticks(), rotation=0)
    axes.xaxis.get_minor_ticks()[0].set_visible(False)
    axes.xaxis.get_minor_ticks()[-1].set_visible(False)
    axes.set_ylabel('Proximity sets without trackers')
    axes.yaxis.set_major_formatter(PercentFormatter(xmax=1))
    axes.legend(ncol=3, loc='upper center', bbox_to_anchor=(0.5, 1.1))

    axes.figure.savefig(join_with_plots_path(f"NO-{input_path.stem}"), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def plot_trackers(input_path: Path) -> None:
    """Plot the number of script inclusions per proximity set via the union and intersection of sources."""
    with open(input_path) as file:
        results = json.load(file)

    data = DataFrame()
    for agg in 'Union', 'Intersection':
        trackers_counts = []
        for ts in TIMESTAMPS:
            trackers = set()
            for tid in results:
                if str(ts) in results[tid]:
                    trackers.update(results[tid][str(ts)][agg])
            trackers_counts.append(len(trackers))
        data[agg] = trackers_counts

    axes = data.plot(style=STYLE, color=COLORS, grid=True)

    axes.set_xlabel('Timestamp')
    axes.set_xticks(*get_year_ticks(), rotation=0)
    axes.xaxis.get_minor_ticks()[0].set_visible(False)
    axes.xaxis.get_minor_ticks()[-1].set_visible(False)
    axes.set_ylabel('Unique trackers')
    axes.legend(ncol=3, loc='upper center', bbox_to_anchor=(0.5, 1.1))

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify(xtick_minor_visible=True)
def main():
    plot_script_inclusions(join_with_json_path(f"SCRIPT-INCLUSIONS-{ARCHIVE_TABLE_NAME}.json"))
    plot_script_inclusion_bounds(join_with_json_path(f"SCRIPT-INCLUSIONS-PROXIMITY-SETS-{10}.json"))
    plot_domains_without_trackers(join_with_json_path(f"TRACKERS-PROXIMITY-SETS-{10}.json"))
    plot_trackers(join_with_json_path(f"TRACKERS-PROXIMITY-SETS-{10}.json"))


if __name__ == '__main__':
    main()
