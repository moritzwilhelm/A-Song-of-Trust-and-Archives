from pathlib import Path

from matplotlib import pyplot as plt
from matplotlib.ticker import PercentFormatter
from pandas import read_json, DataFrame

from configs.utils import join_with_json_path, json_to_plots_path
from plotting.plotting_utils import COLORS, latexify

CATEGORY_ABBREVIATIONS = {
    'X-Frame-Options': 'XFO',
    'Content-Security-Policy-FA': 'CSP-FA',
    'Content-Security-Policy-XSS': 'CSP-XSS',
    'Content-Security-Policy-TLS': 'CSP-TLS',
    'Strict-Transport-Security': 'HSTS',
    'Referrer-Policy': 'RP',
    'Permissions-Policy': 'PP',
    'Cross-Origin-Opener-Policy': 'COOP',
    'Cross-Origin-Resource-Policy': 'CORP',
    'Cross-Origin-Embedder-Policy': 'COEP'
}


def plot_feature_importance(input_path: Path):
    """Plot the best features to reduce the Gini impurity per proximity set."""
    with open(input_path) as file:
        data = read_json(file, orient='index')

    df = DataFrame()
    for column in data.columns[:-1]:
        df[column.replace('_', ' ').capitalize()] = data[column] / data['total']

    axes = df.plot.bar(color=COLORS, grid=True, ylim=(0.0, 1.025), rot=45)
    axes.xaxis.set_major_formatter(lambda label, _: CATEGORY_ABBREVIATIONS[data.index[label]])
    axes.set_ylabel('Inconsistent Proximity Sets')
    axes.yaxis.set_major_formatter(PercentFormatter(xmax=1.0))
    axes.legend(ncol=5, loc='upper center', bbox_to_anchor=(0.5, 1.1))

    axes.figure.savefig(json_to_plots_path(input_path), bbox_inches='tight', dpi=300)

    axes.figure.show()
    plt.close()


@latexify()
def main():
    plot_feature_importance(join_with_json_path(f"FEATURES-PROXIMITY-SETS-{10}.json"))


if __name__ == '__main__':
    main()
