from seaborn import color_palette

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
