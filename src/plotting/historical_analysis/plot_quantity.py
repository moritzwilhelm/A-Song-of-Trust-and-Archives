import json
from os.path import commonprefix
from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import PercentFormatter
from pandas import DataFrame

from configs.crawling import NUMBER_URLS, TIMESTAMPS
from configs.random_sample_tranco import RANDOM_SAMPLING_TABLE_NAME
from configs.utils import join_with_json_path, join_with_plots_path, json_to_plots_path
from data_collection.collect_archive_data import TABLE_NAME
from plotting.plotting_utils import COLORS, get_year_ticks, latexify


def plot_hits(hits_input_path: Path, fresh_hits_input_path: Path) -> None:
    """Plot the amount of archive hits in both `input_paths` and save the figure at `output_path`."""
    with open(hits_input_path) as file:
        hits = DataFrame.from_dict(json.load(file), orient='index', columns=['Hits'])
    with open(fresh_hits_input_path) as file:
        fresh_hits = DataFrame.from_dict(json.load(file), orient='index', columns=['Fresh Hits'])

    data = hits.merge(fresh_hits, left_index=True, right_index=True)

    axes = data.plot.bar(color=COLORS, grid=True, ylim=(0, NUMBER_URLS))
    axes.legend(loc='lower right')
    axes.set_xlabel('Timestamp')
    axes.set_xticks(*get_year_ticks(), rotation=0)
    axes.set_ylabel('Number of domains')

    output_filename = f"{commonprefix((hits_input_path.name, fresh_hits_input_path.name)).rstrip('-')}.png"
    axes.figure.savefig(join_with_plots_path(output_filename), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def plot_drifts(input_path: Path) -> None:
    """Plot the temporal drifts between archived date and requested date."""
    with open(input_path) as file:
        data = pd.read_json(file, orient='index').transpose()

    axes = data.plot.box(
        grid=True,
        ylim=(-42, 42),
        boxprops=dict(linestyle='-', linewidth=1, color=COLORS[0]),
        whiskerprops=dict(linestyle='dotted', linewidth=1, color=COLORS[1]),
        medianprops=dict(linestyle='-', linewidth=1, color=COLORS[2]),
        capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
        flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4]),
        # showfliers=False
    )
    axes.set_xlabel('Timestamp')
    axes.set_xticks(*get_year_ticks(1), rotation=0)
    axes.set_ylabel('Temporal drift in days')
    axes.set_yticks(range(-42, 43, 7))

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def plot_hits_per_buckets(input_path: Path):
    """Plot the number of archive hits per 100k bucket."""
    with open(input_path) as file:
        data = pd.read_json(file, orient='index')
        data = pd.concat([data, pd.DataFrame(index=data.index[:4 - len(TIMESTAMPS) % 4])])

    for i in range(0, len(data), 8):
        axes = data.iloc[i:i + 8].plot.bar(color=COLORS, grid=True, ylim=(0, NUMBER_URLS // 10))
        axes.set_xlabel('Timestamp')
        axes.set_xticks([0, 4], sorted({date.year for date in TIMESTAMPS[i:i + 8]}), rotation=0)
        axes.set_ylabel('Hits' if 'None-w' in input_path.name else 'Fresh Hits')
        axes.set_yticks(range(0, (NUMBER_URLS // 10) + 1, NUMBER_URLS // 100))
        axes.yaxis.set_major_formatter(PercentFormatter(xmax=NUMBER_URLS / 10))
        axes.legend([f"{bucket}k" for bucket in data.columns], ncol=5, loc='upper center', bbox_to_anchor=(0.5, 1.16))
        axes.figure.savefig(json_to_plots_path(input_path, f".{TIMESTAMPS[i].year}-{TIMESTAMPS[i + 4].year}.png"),
                            bbox_inches='tight', dpi=300)

        axes.figure.show()
        plt.close()


@latexify(xtick_minor_visible=True)
def main():
    for table_name in TABLE_NAME, RANDOM_SAMPLING_TABLE_NAME:
        plot_hits(
            join_with_json_path(f"QUANTITY-{table_name}-None-w-tolerance.json"),
            join_with_json_path(f"QUANTITY-{table_name}-6-w-tolerance.json")
        )

        for tolerance in None, 6:
            plot_drifts(
                join_with_json_path(f"DRIFTS-{table_name}-{tolerance}-w-tolerance.json")
            )

    for tolerance in None, 6:
        plot_hits_per_buckets(
            join_with_json_path(f"BUCKETS-{RANDOM_SAMPLING_TABLE_NAME}-{tolerance}-w-tolerance.json")
        )


if __name__ == '__main__':
    main()
