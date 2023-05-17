from enum import Enum


class Status(str, Enum):
    """Describes the status of an archived website snapshot."""
    MISSING = 'MISSING'
    ADDED = 'ADDED'
    REMOVED = 'REMOVED'
    MODIFIED = 'MODIFIED'
    UNMODIFIED = 'UNMODIFIED'
