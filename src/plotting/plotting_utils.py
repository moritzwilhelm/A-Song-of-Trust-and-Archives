import functools
from math import sqrt
from typing import Tuple, List, Optional, Callable

import matplotlib
import seaborn as sns

from configs.crawling import TIMESTAMPS

HEADER_ABBREVIATION = {
    'X-Frame-Options': 'XFO',
    'Content-Security-Policy': 'CSP',
    'Strict-Transport-Security': 'HSTS',
    'Referrer-Policy': 'RP',
    'Permissions-Policy': 'PP',
    'Cross-Origin-Opener-Policy': 'COOP',
    'Cross-Origin-Resource-Policy': 'CORP',
    'Cross-Origin-Embedder-Policy': 'COEP'
}

STYLE = ['s-', 'o-', '^-', 's--', 'o--', '^--', 's:', 'o:', '^:', 's-.']

COLORS = sns.color_palette('colorblind')


def get_year_ticks(start: int = 0) -> Tuple[range, List[int]]:
    return range(start, len(TIMESTAMPS), 4), sorted({date.year for date in TIMESTAMPS})


def _latexify(fig_width: Optional[float] = None,
              fig_height: Optional[float] = None,
              xtick_minor_visible: bool = False) -> None:
    """Set up matplotlib and seaborn's RC params for LaTeX plotting."""
    if fig_width is None:
        fig_width_pt = 412.56497
        inches_per_pt = 1.0 / 72.27
        fig_width = fig_width_pt * inches_per_pt

    if fig_height is None:
        golden_mean = (sqrt(5) - 1.0) / 2.0  # Aesthetic ratio
        fig_height = fig_width * golden_mean  # * 0.75  # height in inches

    max_height_inches = 8.0
    if fig_height > max_height_inches:
        print("WARNING: fig_height too large:" + str(fig_height) +
              "so will reduce to" + str(max_height_inches) + "inches.")
        fig_height = max_height_inches

    fontsize = 8
    legendsize = 8

    params = {
        'text.latex.preamble': ['\\usepackage{gensymb}', '\\usepackage{subscript}'],
        'font.family': 'serif',
        'font.size': fontsize,
        'text.usetex': True,
        'figure.figsize': [fig_width, fig_height],
        'axes.labelsize': fontsize,
        'axes.titlesize': fontsize,
        'axes.linewidth': '1',
        'axes.grid': False,
        'xtick.labelsize': fontsize,
        'xtick.minor.visible': xtick_minor_visible,
        'ytick.labelsize': fontsize,
        'grid.linewidth': '1',
        # 'grid.alpha': 0.5,
        'legend.fontsize': legendsize,
        'legend.fancybox': True,
        'legend.frameon': True,
    }

    sns.set(style='ticks', context='paper', rc=params)

    matplotlib.rcParams.update(params)


def latexify(fig_width: Optional[float] = None,
             fig_height: Optional[float] = None,
             xtick_minor_visible: bool = False) -> Callable:
    def latexify_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _latexify(fig_width, fig_height, xtick_minor_visible)
            return func(*args, **kwargs)

        return wrapper

    return latexify_decorator
