from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt

from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import COLORS, get_year_ticks, latexify


def plot_set_size(file_path: Path) -> None:
    """Plot the computed proximity set statistics."""
    with open(file_path) as file:
        data = pd.read_json(file, orient='index')

    data['Set size'] = data['Set size'].apply(lambda row: [set_size for set_size in row if set_size > 1])

    axes = data.explode('Set size').reset_index().pivot(columns='index', values='Set size').plot.box(
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
    axes.set_ylabel('Set size')
    axes.set_yticks(range(11))

    axes.figure.savefig(json_to_plots_path(file_path, '.SET-SIZE.png'), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify(xtick_minor_visible=True)
def main():
    plot_set_size(join_with_json_path(f"STATS-PROXIMITY-SETS-{10}.json"))


if __name__ == '__main__':
    main()
