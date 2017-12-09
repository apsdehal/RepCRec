"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class LockType(Enum):
    """
    Types of lock present
    """
    READ = 0
    WRITE = 1
