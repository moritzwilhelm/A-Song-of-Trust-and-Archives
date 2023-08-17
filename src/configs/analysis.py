RELEVANT_HEADERS = (
    'X-Frame-Options',
    'Content-Security-Policy',
    'Strict-Transport-Security',
    'Referrer-Policy',
    'Permissions-Policy',
    'Cross-Origin-Opener-Policy',
    'Cross-Origin-Resource-Policy',
    'Cross-Origin-Embedder-Policy'
)

INTERNET_ARCHIVE_SOURCE_HEADER = 'X-Archive-Src'
INTERNET_ARCHIVE_HEADER_PREFIX = 'X-Archive-Orig-'
INTERNET_ARCHIVE_END_URL_REGEX = r'^(?:[^\/]*\/){5}(.*)'

MEMENTO_HEADER = 'Memento-Datetime'
MEMENTO_HEADER_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
