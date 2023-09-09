from enum import Enum
from typing import TypeVar

E = TypeVar('E', bound=Enum)


def max_enum(first: E, second: E) -> E:
    """Compute the maximum enum based on their value."""
    return first if first.value >= second.value else second


class HSTSAge(Enum):
    DISABLED = -1
    ABSENT = 0
    LOW = 1
    BIG = 2


class HSTSSub(Enum):
    ABSENT = 0
    ACTIVE = 1


class HSTSPreload(Enum):
    ABSENT = 0
    ACTIVE = 1


class XFO(Enum):
    UNSAFE = 0
    SAMEORIGIN = 1
    DENY = 2


class CspXSS(Enum):
    UNSAFE = 0
    SAFE = 1


class CspFA(Enum):
    UNSAFE = 0
    CONSTRAINED = 1
    SELF = 2
    NONE = 3


class CspTLS(Enum):
    UNSAFE = 0
    BLOCK_ALL_MIXED_CONTENT = 1
    UPGRADE_INSECURE_REQUESTS = 2


class RP(Enum):
    UNSAFE_URL = 0
    SAME_ORIGIN = 1
    NO_REFERRER = 2
    NO_REFERRER_WHEN_DOWNGRADE = 3
    ORIGIN = 4
    ORIGIN_WHEN_CROSS_ORIGIN = 5
    STRICT_ORIGIN = 6
    STRICT_ORIGIN_WHEN_CROSS_ORIGIN = 7


class COOP(Enum):
    UNSAFE_NONE = 0
    SAME_ORIGIN = 1
    SAME_ORIGIN_ALLOW_POPUPS = 2


class CORP(Enum):
    CROSS_ORIGIN = 0
    SAME_SITE = 1
    SAME_ORIGIN = 2


class COEP(Enum):
    UNSAFE_NONE = 0
    REQUIRE_CORP = 1
    CREDENTIALLESS = 2
