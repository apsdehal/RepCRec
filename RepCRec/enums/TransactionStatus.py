"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class TransactionStatus(Enum):
    """
    Tells status of a particular transaction
    """
    RUNNING = 0
    WAITING = 1
    BLOCKED = 2
    ABORTED = 3
    COMMITTED = 4
