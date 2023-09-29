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
        medianprops=dict(linestyle='-', linewidth=1.5, color=COLORS[4]),
        capprops=dict(linestyle='-', linewidth=1, color=COLORS[3]),
        flierprops=dict(linestyle='none', markersize=1, linewidth=0, color=COLORS[4])
    )
    axes.set_xticks(*get_year_ticks(1), rotation=0)
    axes.set_ylabel('Neighborhood size')
    axes.set_yticks(range(11))

    axes.figure.savefig(json_to_plots_path(file_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


def build_neighborhood_sizes_table(file_path: Path, print_stats=True) -> None:
    """Build a LaTeX table about neighborhood coverage."""
    with open(file_path) as file:
        data = read_json(file, orient='index')

    df = DataFrame()
    df['Has one fresh hit'] = data['size'].apply(lambda row: sum(set_size >= 1 for set_size in row))
    df['Has neighborhood'] = data['size'].apply(lambda row: sum(set_size >= 2 for set_size in row))

    if print_stats:
        print('Number of snapshots:', data['size'].apply(sum).sum())
        neighborhood_count = df['Has one fresh hit'].sum()
        print('Number of neighborhoods:', neighborhood_count)
        true_neighborhood_count = df['Has neighborhood'].sum()
        print('Number of (non-pseudo) neighborhoods:', true_neighborhood_count,
              f"({true_neighborhood_count / neighborhood_count:.2%})")

        sizes = sorted(value for row in data['size'] for value in row if value >= 2)
        print("Average neighborhood size:", f"{sum(sizes) / len(sizes):.2f}")
        mid = len(sizes) // 2
        print("Median neighborhood size:", f"{(sizes[mid] + sizes[~mid]) / 2:.1f}")
        print()

    df['Has neighborhood'] = df.apply(
        lambda row: f"{row['Has neighborhood']:,} ({row['Has neighborhood'] / row['Has one fresh hit'] * 100:.2f}\%)",
        axis=1
    )
    df['Has one fresh hit'] = df['Has one fresh hit'].apply(lambda value: f"{value:,}")

    print(df.to_latex())


def build_snapshot_contributors_table(file_path: Path) -> None:
    """Build a LaTex table listing the top 10 contributors."""
    with open(file_path) as file:
        data = read_json(file, orient='index').reset_index(names=['Contributor'])

    total = sum(data[0])
    data['Number of Snapshots'] = data[0].apply(lambda value: f"{value:,} ({value / total * 100:.2f}\%)")

    top10 = data.nlargest(10, 0)
    remainder = total - top10[0].sum()
    top10.loc[-1] = ([r"\emph{Remainder}", None, f"{remainder:,} ({remainder / total * 100:.2f}\%)"])
    top10.index += 1
    top10.loc[-1] = ([r"\emph{Total}", None, f"{total:,} (100.00\%)"])
    top10.index += 1
    print(top10.to_latex(index=False, columns=['Contributor', 'Number of Snapshots'], column_format='c|r'))


@latexify(xtick_minor_visible=True)
def main():
    plot_neighborhood_sizes(join_with_json_path(f"SIZES-NEIGHBORHOODS.{10}.json"))
    build_neighborhood_sizes_table(join_with_json_path(f"SIZES-NEIGHBORHOODS.{10}.json"))
    build_snapshot_contributors_table(join_with_json_path(f"CONTRIBUTORS-NEIGHBORHOODS.{10}.json"))


if __name__ == '__main__':
    main()
