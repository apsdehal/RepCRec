"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class LockAcquireStatus(Enum):
    """
    Type of output returns when one tries to acquire
    a lock through site manager
    """
    ALL_SITES_DOWN = 0
    NO_LOCK = 1
    GOT_LOCK = 2
    GOT_LOCK_RECOVERING = 3
