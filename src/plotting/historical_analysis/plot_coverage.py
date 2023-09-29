import json
from os.path import commonprefix
from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.ticker import PercentFormatter
from numpy import arange
from pandas import DataFrame, read_json

from configs.crawling import NUMBER_URLS, TIMESTAMPS
from configs.files.random_sample_tranco import RANDOM_SAMPLING_TABLE_NAME
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

    axes = data.plot.bar(color=COLORS, grid=True, ylim=(0, NUMBER_URLS), width=0.75)
    axes.legend(loc='lower right')
    axes.set_xticks(*get_year_ticks(), rotation=0)
    axes.set_ylabel('Number of domains')

    axes.figure.savefig(join_with_plots_path(f"{commonprefix((hits_input_path.name, fresh_hits_input_path.name))}png"),
                        bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def plot_drifts(input_path: Path, tolerance: int | None) -> None:
    """Plot the temporal drifts between archived date and requested date."""
    with open(input_path) as file:
        data = read_json(file, orient='index').transpose()

    axes = data.plot.box(
        grid=True,
        ylim=(-tolerance - 1, tolerance + 1) if tolerance is not None else None,
        boxprops=dict(linestyle='-', linewidth=1, color=COLORS[0]),
        whiskerprops=dict(linestyle='dotted', linewidth=1, color=COLORS[1]),
        medianprops=dict(linestyle='-', linewidth=1, color=COLORS[4]),
        capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
        flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4]),
        showfliers=tolerance is not None
    )

    axes.set_xticks(*get_year_ticks(1), rotation=0)
    axes.set_ylabel('Temporal drift in days')
    if tolerance is not None:
        axes.set_yticks(range(-tolerance, tolerance + 1, 7))

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify(fig_width=412.56497 * (1.0 / 72.27), fig_height=(412.56497 * (1.0 / 72.27)) * 1.75, ytick_minor_visible=True)
def plot_hits_per_buckets(hits_input_path: Path, fresh_hits_input_path: Path) -> None:
    """Plot the number of archive hits per 100k bucket."""
    with open(hits_input_path) as file:
        hits = read_json(file, orient='index')

    with open(fresh_hits_input_path) as file:
        fresh_hits = read_json(file, orient='index')

    def build_axes(df: DataFrame, xmax: int):
        axes = df.plot.barh(width=0.8, color=COLORS, grid=True, xlim=(0, xmax))

        axes.set_xlabel('Hits' if df is hits else 'Fresh Hits')
        axes.set_xticks(arange(0, xmax + (xmax / 10), xmax / 10))
        axes.xaxis.set_major_formatter(PercentFormatter(xmax=xmax))
        axes.set_yticks(*get_year_ticks())
        axes.invert_yaxis()
        axes.legend([f"{bucket}k" for bucket in df.columns], ncol=5, loc='upper center', bbox_to_anchor=(0.5, 1.05))
        return axes

    axes = build_axes(hits, NUMBER_URLS // 10)
    axes.figure.savefig(json_to_plots_path(hits_input_path, f".{TIMESTAMPS[0].year}-{TIMESTAMPS[-1].year}.png"),
                        bbox_inches='tight', dpi=300)
    axes.figure.show()

    axes = build_axes(fresh_hits / hits, 1)
    axes.figure.savefig(json_to_plots_path(fresh_hits_input_path, f".{TIMESTAMPS[0].year}-{TIMESTAMPS[-1].year}.png"),
                        bbox_inches='tight', dpi=300)
    axes.figure.show()
    plt.close()


@latexify(xtick_minor_visible=True)
def main():
    for table_name in TABLE_NAME, RANDOM_SAMPLING_TABLE_NAME:
        plot_hits(
            join_with_json_path(f"COVERAGE-{table_name}.None.json"),
            join_with_json_path(f"COVERAGE-{table_name}.42.json")
        )

        for tolerance in None, 7 * 6:
            plot_drifts(join_with_json_path(f"DRIFTS-{table_name}.{tolerance}.json"), tolerance)

    plot_hits_per_buckets(join_with_json_path(f"BUCKETS-{RANDOM_SAMPLING_TABLE_NAME}.None.json"),
                          join_with_json_path(f"BUCKETS-{RANDOM_SAMPLING_TABLE_NAME}.42.json"))


if __name__ == '__main__':
    main()
