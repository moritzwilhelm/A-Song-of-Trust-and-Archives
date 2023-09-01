from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame

from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import COLORS, get_year_ticks, latexify


def plot_set_size(file_path: Path) -> None:
    """Plot the computed proximity set statistics."""
    with open(file_path) as file:
        data = pd.read_json(file, orient='index')

    data['Set size'] = data['Set size'].apply(lambda row: [set_size for set_size in row if set_size > 1])

    axes = data.explode('Set size').reset_index().pivot(columns='index', values='Set size').astype(float).plot.box(
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


def build_set_count_table(file_path: Path) -> None:
    """Build a latex table about proximity set statistics."""
    with open(file_path) as file:
        data = pd.read_json(file, orient='index')

    df = DataFrame()
    df['At least one fresh hit'] = data['Set size'].apply(lambda row: sum(set_size >= 1 for set_size in row))
    df['Has neighborhood'] = data['Set size'].apply(lambda row: sum(set_size >= 2 for set_size in row))
    df['Has neighborhood'] = df.apply(
        lambda row: f"{row['Has neighborhood']} ({row['Has neighborhood'] / row['At least one fresh hit'] * 100:.2f}\%)",
        axis=1
    )
    print(df.to_latex())

    df['Set size >= 1'] = df['At least one fresh hit']
    for i in range(2, 11):
        df[f"Set size >= {i}"] = data['Set size'].apply(lambda row: sum(set_size >= i for set_size in row))
        df[f"Set size >= {i}"] = df.apply(
            lambda row: f"{row[f'Set size >= {i}']} ({row[f'Set size >= {i}'] / row['Set size >= 1'] * 100:.2f}\%)",
            axis=1
        )

    del df['At least one fresh hit'], df['Has neighborhood']
    print(df.to_latex())


@latexify(xtick_minor_visible=True)
def main():
    plot_set_size(join_with_json_path(f"STATS-PROXIMITY-SETS-{10}.json"))
    build_set_count_table(join_with_json_path(f"STATS-PROXIMITY-SETS-{10}.json"))


if __name__ == '__main__':
    main()
