RELEVANT_HEADERS = (
    'Strict-Transport-Security',
    'X-Frame-Options',
    'Content-Security-Policy',
    'Permissions-Policy',
    'Referrer-Policy',
    'Cross-Origin-Opener-Policy',
    'Cross-Origin-Resource-Policy',
    'Cross-Origin-Embedder-Policy'
)

SECURITY_MECHANISM_HEADERS = {
    'Strict-Transport-Security': 'Strict-Transport-Security',
    'X-Frame-Options': 'X-Frame-Options',
    'Content-Security-Policy::XSS': 'Content-Security-Policy',
    'Content-Security-Policy::FA': 'Content-Security-Policy',
    'Content-Security-Policy::TLS': 'Content-Security-Policy',
    'Content-Security-Policy': 'Content-Security-Policy',
    'Permissions-Policy': 'Permissions-Policy',
    'Referrer-Policy': 'Referrer-Policy',
    'Cross-Origin-Opener-Policy': 'Cross-Origin-Opener-Policy',
    'Cross-Origin-Resource-Policy': 'Cross-Origin-Resource-Policy',
    'Cross-Origin-Embedder-Policy': 'Cross-Origin-Embedder-Policy'
}

INTERNET_ARCHIVE_SOURCE_HEADER = 'X-Archive-Src'
INTERNET_ARCHIVE_HEADER_PREFIX = 'X-Archive-Orig-'
INTERNET_ARCHIVE_END_URL_REGEX = r'^(?:[^\/]*\/){5}(.*)'

MEMENTO_HEADER = 'Memento-Datetime'
MEMENTO_HEADER_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
