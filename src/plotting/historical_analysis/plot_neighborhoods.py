from pathlib import Path

from matplotlib import pyplot as plt
from pandas import DataFrame, read_json

from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import COLORS, get_year_ticks, latexify


def plot_neighborhood_sizes(file_path: Path) -> None:
    """Plot the computed neighborhood sizes."""
    with open(file_path) as file:
        data = read_json(file, orient='index')

    data['size'] = data['size'].apply(lambda row: [size for size in row if size > 1])

    axes = data.explode('size').reset_index().pivot(columns='index', values='size').astype(float).plot.box(
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
    axes.set_ylabel('Neighborhood size')
    axes.set_yticks(range(11))

    axes.figure.savefig(json_to_plots_path(file_path, '.SIZE.png'), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def build_set_count_table(file_path: Path) -> None:
    """Build a latex table about neighborhood coverage."""
    with open(file_path) as file:
        data = read_json(file, orient='index')

    df = DataFrame()
    df['Has one fresh hit'] = data['size'].apply(lambda row: sum(set_size >= 1 for set_size in row))
    df['Has neighborhood'] = data['size'].apply(lambda row: sum(set_size >= 2 for set_size in row))
    df['Has neighborhood'] = df.apply(
        lambda row: f"{row['Has neighborhood']} ({row['Has neighborhood'] / row['Has one fresh hit'] * 100:.2f}\%)",
        axis=1
    )

    print(df.to_latex())


@latexify(xtick_minor_visible=True)
def main():
    plot_neighborhood_sizes(join_with_json_path(f"SIZES-NEIGHBORHOODS.{10}.json"))
    build_set_count_table(join_with_json_path(f"SIZES-NEIGHBORHOODS.{10}.json"))


if __name__ == '__main__':
    main()
