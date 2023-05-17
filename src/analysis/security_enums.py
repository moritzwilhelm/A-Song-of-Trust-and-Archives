from enum import Enum
from typing import TypeVar

E = TypeVar('E', bound=Enum)


def max_enum(first: E, second: E) -> E:
    """Compute the maximum enum based on their value."""
    return first if first.value >= second.value else second


class XFO(Enum):
    UNSAFE = 0
    SELF = 1
    NONE = 2


class CspFA(Enum):
    UNSAFE = 0
    CONSTRAINED = 1
    SELF = 2
    NONE = 3


class CspXSS(Enum):
    UNSAFE = 0
    SAFE = 1


class CspTLS(Enum):
    UNSAFE = 0
    ENABLED = 1


class HSTSAge(Enum):
    DISABLE = -1
    UNSAFE = 0
    LOW = 1
    BIG = 2


class HSTSSub(Enum):
    UNSAFE = 0
    SAFE = 1


class HSTSPreload(Enum):
    ABSENT = 0
    ACTIVE = 1


class RP(Enum):
    UNSAFE = 0
    SAFE = 1


class COOP(Enum):
    UNSAFE = 0
    SAFE = 1


class CORP(Enum):
    UNSAFE = 0
    SAFE = 1


class COEP(Enum):
    UNSAFE = 0
    SAFE = 1
