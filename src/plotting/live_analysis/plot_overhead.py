from pathlib import Path

from matplotlib import pyplot as plt
from pandas import read_json

from analysis.live.analyze_disagreement import ARCHIVE_TABLE_NAME
from configs.utils import join_with_json_path
from configs.utils import json_to_plots_path
from data_collection.collect_live_data import TABLE_NAME as LIVE_TABLE_NAME
from plotting.plotting_utils import COLORS, latexify


def plot_overhead(input_path: Path) -> None:
    """Plot the request overhead of the Internet Archive."""
    with open(input_path) as file:
        data = read_json(file, orient='index').transpose()

    for column in data:
        print(f"{column:24s}\t{data[column].mean() / 1000:.2f}s")

    axes = data.plot.box(
        grid=True,
        boxprops=dict(linestyle='-', linewidth=1, color=COLORS[0]),
        whiskerprops=dict(linestyle='dotted', linewidth=1, color=COLORS[1]),
        medianprops=dict(linestyle='-', linewidth=1, color=COLORS[2]),
        capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
        flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4]),
        showfliers=False
    )
    axes.set_ylabel('Response time in ms')

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify()
def main():
    plot_overhead(join_with_json_path(f"OVERHEAD-{LIVE_TABLE_NAME}-{ARCHIVE_TABLE_NAME}.json"))


if __name__ == '__main__':
    main()
