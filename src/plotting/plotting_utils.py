import functools
from math import sqrt
from typing import Callable

import matplotlib
import seaborn as sns

from configs.crawling import TIMESTAMPS

HEADER_ABBREVIATION = {
    'Strict-Transport-Security': 'HSTS',
    'X-Frame-Options': 'XFO',
    'Content-Security-Policy': 'CSP',
    'Permissions-Policy': 'PP',
    'Referrer-Policy': 'RP',
    'Cross-Origin-Opener-Policy': 'COOP',
    'Cross-Origin-Resource-Policy': 'CORP',
    'Cross-Origin-Embedder-Policy': 'COEP'
}

STYLE = ['s-', 'o-', '^-', 's--', 'o--', '^--', 's:', 'o:', '^:', 's-.']

COLORS = sns.color_palette('colorblind')


def get_year_ticks(start: int = 0) -> tuple[range, list[int]]:
    return range(start, len(TIMESTAMPS), 4), sorted({date.year for date in TIMESTAMPS})


def _latexify(fig_width: float | None = None,
              fig_height: float | None = None,
              xtick_minor_visible: bool = False,
              ytick_minor_visible: bool = False) -> None:
    """Set up matplotlib and seaborn's RC params for LaTeX plotting."""
    if fig_width is None:
        fig_width_pt = 412.56497
        inches_per_pt = 1.0 / 72.27
        fig_width = fig_width_pt * inches_per_pt

    if fig_height is None:
        golden_mean = (sqrt(5) - 1.0) / 2.0  # Aesthetic ratio
        fig_height = fig_width * golden_mean  # * 0.75  # height in inches

    fontsize = 8
    legendsize = 8

    params = {
        'text.usetex': True,
        'font.family': 'serif',
        'font.size': fontsize,
        'figure.autolayout': True,
        'figure.figsize': [fig_width, fig_height],
        'axes.labelsize': fontsize,
        'axes.titlesize': fontsize,
        'axes.linewidth': '1',
        'axes.grid': False,
        'xtick.labelsize': fontsize,
        'xtick.minor.visible': xtick_minor_visible,
        'ytick.labelsize': fontsize,
        'ytick.minor.visible': ytick_minor_visible,
        'grid.linewidth': '1',
        # 'grid.alpha': 0.5,
        'legend.fontsize': legendsize,
        'legend.fancybox': True,
        'legend.frameon': True,
    }

    sns.set(style='ticks', context='paper', rc=params)

    matplotlib.rcParams.update(params)


def latexify(fig_width: float | None = None,
             fig_height: float | None = None,
             xtick_minor_visible: bool = False,
             ytick_minor_visible: bool = False) -> Callable:
    def latexify_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _latexify(fig_width, fig_height, xtick_minor_visible, ytick_minor_visible)
            return func(*args, **kwargs)

        return wrapper

    return latexify_decorator
