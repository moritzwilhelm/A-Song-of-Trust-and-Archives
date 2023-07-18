from pathlib import Path

import pandas as pd
from matplotlib.ticker import MaxNLocator

from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import COLORS, get_year_ticks


def plot_stats(file_path: Path):
    """Plot the computed proximity set statistics."""
    with open(file_path) as file:
        data = pd.read_json(file, orient='index')

    for column in data:
        set_sizes = data.explode(column).reset_index().pivot(columns='index', values=column)
        axes = set_sizes.plot.box(
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
        axes.set_ylabel(column)
        axes.yaxis.set_major_locator(MaxNLocator(integer=True))

        output_path = json_to_plots_path(file_path, f".{'-'.join(column.capitalize().split())}.pdf")
        axes.figure.savefig(output_path, bbox_inches='tight', dpi=300)

        axes.figure.show()


def main():
    plot_stats(join_with_json_path(f"STATS-PROXIMITY-SETS-{10}.json"))
    plot_stats(join_with_json_path(f"STATS-PROXIMITY-SETS-STRICT-{10}.json"))


if __name__ == '__main__':
    main()
