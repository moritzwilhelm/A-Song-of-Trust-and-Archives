from datetime import datetime

from pytz import utc

INTERNET_ARCHIVE_URL = 'https://web.archive.org/web/{timestamp}/{url}'
INTERNET_ARCHIVE_TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'

NUMBER_URLS = 20_000
PREFIX = 'http://www.'
USER_AGENT = \
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'

TODAY = datetime.now(utc)
TIMESTAMPS = tuple(
    datetime(year, month, 15, 12, tzinfo=utc)
    for year in range(2016, TODAY.year + 1)
    for month in [1, 4, 7, 10]
    if datetime(year, month, 15, 12, tzinfo=utc) <= TODAY
)

SOCKS_PROXIES = {
    '1221': 'ec2-user@3.122.230.202',
    '1222': 'ec2-user@35.158.119.135',
    '1223': 'ec2-user@3.120.244.133',
    '1224': 'ec2-user@3.67.98.212',
    '1225': 'ec2-user@3.73.55.74',
    '1226': 'ec2-user@3.66.223.69',
    '1227': 'ec2-user@18.195.111.68',
    '1228': 'ec2-user@18.196.239.251',
    '1229': 'ec2-user@3.74.150.222',
    '1230': 'ec2-user@35.158.163.248',
    '1231': 'ec2-user@18.184.3.219',
    '1232': 'ec2-user@18.193.222.62',
    '1233': 'ec2-user@3.67.100.19',
    '1234': 'ec2-user@3.74.161.124'
}
