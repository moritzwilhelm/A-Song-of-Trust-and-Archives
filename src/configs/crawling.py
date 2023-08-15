import re
from datetime import datetime

from pytz import utc

WAYBACK_MACHINE_API_PATH = 'https://web.archive.org/web/'
INTERNET_ARCHIVE_URL = 'https://web.archive.org/web/{timestamp}/{url}'
INTERNET_ARCHIVE_TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

NUMBER_URLS = 20_000
PREFIX = 'http://www.'
USER_AGENT = \
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'

TODAY = datetime.now(utc)
FINAL_TIMESTAMP = datetime(2023, 4, 15, 12, tzinfo=utc)
TIMESTAMPS = tuple(
    timestamp
    for year in range(2016, TODAY.year + 1)
    for month in [1, 4, 7, 10]
    if (timestamp := datetime(year, month, 15, 12, tzinfo=utc)) <= FINAL_TIMESTAMP
)

WAYBACK_HEADER_REGEX = re.compile(b"<head>.*<!-- End Wayback Rewrite JS Include -->\n", re.DOTALL)
WAYBACK_COMMENTS_REGEX = re.compile(b"<!--\n     FILE ARCHIVED ON.*", re.DOTALL)
WAYBACK_TOOLBAR_REGEX = re.compile(b"<!-- BEGIN WAYBACK TOOLBAR INSERT -->.*<!-- END WAYBACK TOOLBAR INSERT -->\n ",
                                   re.DOTALL)

SOCKS_PROXIES = {
    '1221': 'maws@srv-r940-02.srv.cispa.saarland',
    '1222': 'maws@srv-r940-03.srv.cispa.saarland',
    '1223': 'maws@swag.cispa.saarland',
    '1224': 'maws@stock.saarland',
    '1225': 'maws@notify.cispa.de'
}
