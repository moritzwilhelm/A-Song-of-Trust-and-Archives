from datetime import timedelta
from functools import cache

from tldextract import extract

from analysis.header_utils import Headers, Origin, parse_origin
from configs.analysis import RELEVANT_HEADERS, INTERNET_ARCHIVE_HEADER_PREFIX
from configs.utils import get_disconnect_tracking_domains, get_easyprivacy_rules

DISCONNECT_TRACKERS = get_disconnect_tracking_domains()
EASYPRIVACY_RULES = get_easyprivacy_rules(supported_options=['script', 'domain', 'third-party'],
                                          skip_unsupported_rules=False)


def parse_archived_headers(headers: Headers) -> Headers:
    """Only keep headers prefixed with 'X-Archive-Orig' and strip the prefix."""
    return Headers({
        header: headers[f"{INTERNET_ARCHIVE_HEADER_PREFIX}{header}"]
        for header in RELEVANT_HEADERS
        if f"{INTERNET_ARCHIVE_HEADER_PREFIX}{header}" in headers
    })


def timedelta_to_days(delta: timedelta) -> float:
    """Translate the provided `timedelta` into days."""
    return delta.total_seconds() / (60 * 60 * 24)


@cache
def parse_hostname(url: str) -> str:
    """Extract the hostname of the given URL."""
    return '.'.join(filter(None, extract(url)))


@cache
def parse_site(url: str) -> str:
    """Extract the hostname of the given URL."""
    return extract(url).registered_domain


@cache
def is_third_party(script_url: str, first_party_origin: Origin) -> bool:
    """Determine if included script is a third-party script."""
    return parse_origin(script_url) != first_party_origin


def is_disconnect_tracker(script_url: str, first_party_origin: Origin) -> bool:
    """Checks if `script_url` can be classified as a tracker according to Disconnect."""
    return (is_third_party(script_url, first_party_origin) and
            (parse_hostname(script_url) in DISCONNECT_TRACKERS or parse_site(script_url) in DISCONNECT_TRACKERS))


def is_easyprivacy_tracker(script_url: str, first_party_origin: Origin) -> bool:
    """Checks if `script_url` can be classified as a tracker according to EasyPrivacy."""
    options = {
        'script': True,
        'third-party': is_third_party(script_url, first_party_origin),
        'domain': parse_hostname(script_url)
    }

    return EASYPRIVACY_RULES.should_block(script_url, options)
