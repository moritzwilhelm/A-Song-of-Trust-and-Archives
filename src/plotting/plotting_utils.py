from typing import Tuple, List

from seaborn import color_palette

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

COLORS = color_palette('colorblind')


def get_year_ticks(start: int = 0) -> Tuple[range, List[int]]:
    return range(start, len(TIMESTAMPS), 4), sorted({date.year for date in TIMESTAMPS})
