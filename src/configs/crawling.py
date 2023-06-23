from datetime import datetime

from pytz import utc

NUMBER_URLS = 20_000
PREFIX = 'http://www.'
USER_AGENT = \
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
INTERNET_ARCHIVE_URL = 'https://web.archive.org/web/{timestamp}/{url}'
INTERNET_ARCHIVE_TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

TODAY = datetime.now(utc)
TIMESTAMPS = tuple(
    datetime(year, month, 15, 12, tzinfo=utc)
    for year in range(2016, TODAY.year + 1)
    for month in [1, 4, 7, 10]
    if datetime(year, month, 15, 12, tzinfo=utc) <= TODAY
)
