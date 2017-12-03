from enum import Enum


class LockAcquireStatus(Enum):
    ALL_SITES_DOWN = 0
    NO_LOCK = 1
    GOT_LOCK = 2
