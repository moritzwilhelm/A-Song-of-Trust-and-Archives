import json
from pathlib import Path

from matplotlib.ticker import MaxNLocator
from pandas import DataFrame

from analysis.header_utils import normalize_headers, classify_headers
from configs.analysis import RELEVANT_HEADERS
from configs.crawling import TIMESTAMPS
from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import COLORS, get_year_ticks


def plot_quality(input_path: Path) -> None:
    with open(input_path) as file:
        results = json.load(file)

    data = DataFrame()
    for header in RELEVANT_HEADERS:
        data[header] = [[results[tid][header][str(timestamp)] for tid in results] for timestamp in TIMESTAMPS]
        data[header] = data[header].apply(
            lambda row: [difference_count for deploys, difference_count, set_size in row if deploys and set_size > 1])

        axes = data.explode(header).reset_index().pivot(columns='index', values=header).plot.box(
            grid=True,
            boxprops=dict(linestyle='-', linewidth=1, color=COLORS[0]),
            whiskerprops=dict(linestyle='dotted', linewidth=1, color=COLORS[1]),
            medianprops=dict(linestyle='-', linewidth=1, color=COLORS[2]),
            capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
            flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4]),
            # showfliers=False
        )
        axes.set_xlabel('Timestamp')
        axes.set_xticks(*get_year_ticks(1), rotation=0)
        axes.set_ylabel('Number of differences')
        axes.yaxis.set_major_locator(MaxNLocator(integer=True))

        axes.figure.savefig(json_to_plots_path(input_path, f".{header}.pdf"), bbox_inches='tight', dpi=300)

        axes.figure.show()


def main():
    for aggregation_function in normalize_headers, classify_headers:
        plot_quality(
            join_with_json_path(f"QUALITY-PROXIMITY-SETS-STRICT-{10}.{aggregation_function.__name__}.json"),
        )

        plot_quality(
            join_with_json_path(f"QUALITY-PROXIMITY-SETS-{10}.{aggregation_function.__name__}.json"),
        )


if __name__ == '__main__':
    main()
