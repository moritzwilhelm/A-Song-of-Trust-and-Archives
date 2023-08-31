import re
from datetime import datetime, UTC

# INTERNET ARCHIVE APIs
INTERNET_ARCHIVE_URL = 'https://web.archive.org/web/{timestamp}id_/{url}'
INTERNET_ARCHIVE_TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'
INTERNET_ARCHIVE_METADATA_API = 'https://archive.org/metadata/{source}'

# CONFIGURATION CONSTANTS
NUMBER_URLS = 20_000
URL_PREFIX = 'http://www.'
USER_AGENT = \
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'

# TIMESTAMP CONSTANTS
TODAY = datetime.now(UTC)
FINAL_TIMESTAMP = datetime(2023, 4, 15, 12, tzinfo=UTC)
TIMESTAMPS = tuple(
    timestamp
    for year in range(2016, TODAY.year + 1)
    for month in [1, 4, 7, 10]
    if (timestamp := datetime(year, month, 15, 12, tzinfo=UTC)) <= FINAL_TIMESTAMP
)

# NORMALIZATION REGEXES
WAYBACK_API_REGEX = re.compile(r"https?://web\.archive\.org/web/\d+/https?://.*")

WAYBACK_HEADER_REGEX = re.compile(rb"<head>.*<!-- End Wayback Rewrite JS Include -->[^\n]*\n", re.DOTALL)
WAYBACK_COMMENT_REGEX = re.compile(rb"<!--\n     FILE ARCHIVED ON.*", re.DOTALL)
WAYBACK_TOOLBAR_REGEX = re.compile(
    rb"<!-- BEGIN WAYBACK TOOLBAR INSERT -->.*<!-- END WAYBACK TOOLBAR INSERT -->[^\n]*\n", re.DOTALL)
WAYBACK_SOURCE_REGEX = re.compile(rb"https?://web\.archive\.org/web/\d+[^/]*/(https?://[^)\"']*)")
WAYBACK_PATH_RELATIVE_SOURCE_REGEX = re.compile(rb"//web\.archive\.org/web/\d+[^/]*/https?:(//[^)\"']*)")
WAYBACK_RELATIVE_SOURCE_REGEX = re.compile(rb"/web/\d+[^/]*/https?://[^/]*(/[^)\"']*)")

# SOCKS PROXIES PORT MAPPING
SOCKS_PROXIES = {
    '1221': 'maws@srv-r940-02.srv.cispa.saarland',
    '1222': 'maws@srv-r940-03.srv.cispa.saarland',
    '1223': 'maws@swag.cispa.saarland',
    '1224': 'maws@stock.saarland',
    '1225': 'maws@notify.cispa.de'
}
