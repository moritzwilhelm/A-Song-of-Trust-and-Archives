import json
from collections import defaultdict
from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator
from pandas import DataFrame

from analysis.header_utils import normalize_headers, classify_headers
from configs.analysis import SECURITY_MECHANISM_HEADERS
from configs.crawling import TIMESTAMPS
from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import STYLE, COLORS, get_year_ticks, latexify


def plot_consistency(input_path: Path) -> None:
    """Plot the consistency of header values within each neighborhood per security mechanism."""
    with open(input_path) as file:
        results = json.load(file)

    for mechanism in SECURITY_MECHANISM_HEADERS:
        df = DataFrame()
        df[mechanism] = [[results[tid][mechanism][str(timestamp)] for tid in results] for timestamp in TIMESTAMPS]
        df[mechanism] = df[mechanism].apply(
            lambda row: [value_count for deploys, value_count, size in row if deploys and size > 1]
        )

        df = df.explode(mechanism).reset_index().pivot(columns='index', values=mechanism)

        statistics = DataFrame()
        statistics['Average'] = df.mean(axis=0)
        statistics['Median'] = df.median(axis=0)
        statistics['Maximum'] = df.max(axis=0)

        axes = statistics.plot(style=STYLE, color=COLORS, grid=True)
        axes.scatter(x=0, y=3, color='none')

        axes.set_xlabel('Timestamp')
        axes.set_xticks(*get_year_ticks(), rotation=0)
        axes.xaxis.get_minor_ticks()[0].set_visible(False)
        axes.xaxis.get_minor_ticks()[-1].set_visible(False)
        axes.set_ylabel(f"{'Syntactically' if 'normalize' in input_path.name else 'Semantically'} different values")
        axes.set_yticks(range(max(4, statistics['Maximum'].max() + 1)))
        axes.yaxis.set_major_locator(MaxNLocator(integer=True, steps=[1], min_n_ticks=4))
        axes.legend(ncol=3, loc='upper center', bbox_to_anchor=(0.5, 1.11))

        axes.figure.savefig(json_to_plots_path(input_path, f".{mechanism.replace('::', ' ')}.png"),
                            bbox_inches='tight', dpi=300)

        axes.figure.show()
        plt.close()


def build_table(syntax_input_path: Path, semantics_input_path: Path) -> None:
    with open(syntax_input_path) as file:
        syntax = json.load(file)
    with open(semantics_input_path) as file:
        semantics = json.load(file)

    result = defaultdict(dict)
    for data, key in (syntax, 'SYNTAX'), (semantics, 'SEMANTICS'):
        proximity_sets_deploying_any_header = set()
        proximity_sets_inconsistent_any = set()
        for mechanism in SECURITY_MECHANISM_HEADERS:
            proximity_sets_deploying_header = 0
            proximity_sets_inconsistent = 0
            for tid in data:
                deploys_once = False
                inconsistent_once = False
                for timestamp in TIMESTAMPS:
                    deploys, value_count, set_size = data[tid][mechanism][str(timestamp)]
                    if set_size > 1:
                        proximity_sets_deploying_header += deploys
                        if deploys:
                            proximity_sets_deploying_any_header.add((tid, timestamp))
                        proximity_sets_inconsistent += value_count > 1
                        if value_count > 1:
                            proximity_sets_inconsistent_any.add((tid, timestamp))
                        deploys_once |= deploys
                        inconsistent_once |= value_count > 1

            result[key][mechanism] = proximity_sets_inconsistent
            result['DEPLOYS'][mechanism] = proximity_sets_deploying_header

        proximity_sets_deploying_any_header = len(proximity_sets_deploying_any_header)
        proximity_sets_inconsistent_any = len(proximity_sets_inconsistent_any)
        result[key]['ANY'] = proximity_sets_inconsistent_any
        result['DEPLOYS']['ANY'] = proximity_sets_deploying_any_header

    header_lines = []
    for mechanism, header in SECURITY_MECHANISM_HEADERS.items():
        usage = result['DEPLOYS'][mechanism]
        syn_diff = result['SYNTAX'][mechanism]
        sem_diff = result['SEMANTICS'][mechanism]
        header_lines.append(
            f"\t\t{mechanism} & {usage:,} & "
            fr"{syn_diff:,} ({syn_diff / usage * 100:.2f}\%) & "
            fr"{sem_diff:,} ({sem_diff / usage * 100:.2f}\%) \\"
        )
    header_lines = '\n'.join(header_lines)

    usage = result['DEPLOYS']['ANY']
    syn_diff = result['SYNTAX']['ANY']
    sem_diff = result['SEMANTICS']['ANY']
    any_header_lines = [
        f"\t\t\\textit{{Any header}} & {usage:,} & "
        fr"{syn_diff:,} ({syn_diff / usage * 100:.2f}\%) & "
        fr"{sem_diff:,} ({sem_diff / usage * 100:.2f}\%) \\"
    ]
    any_header_lines = '\n'.join(any_header_lines)

    print(fr"""
\begin{{table}}
    \centering
    \begin{{tabular}}{{l|rrr}}
        & \multicolumn{{3}}{{c}}{{Total ( domains)}} \\
        \midrule
        & \textbf{{usage}} & \textbf{{syn. diff.}} & \textbf{{sem. diff.}} \\
{header_lines}
        \midrule
{any_header_lines}
    \end{{tabular}}
    \caption{{INCONSISTENCIES}}
    \label{{tab:inconsistencies::headers}}
\end{{table}}""")


@latexify(xtick_minor_visible=True)
def main():
    for aggregation_function in normalize_headers, classify_headers:
        plot_consistency(join_with_json_path(f"CONSISTENCY-NEIGHBORHOODS.{10}.{aggregation_function.__name__}.json"))

    build_table(join_with_json_path(f"CONSISTENCY-NEIGHBORHOODS.{10}.{normalize_headers.__name__}.json"),
                join_with_json_path(f"CONSISTENCY-NEIGHBORHOODS.{10}.{classify_headers.__name__}.json"))


if __name__ == '__main__':
    main()
