from enum import Enum


class TransactionStatus(Enum):
    RUNNING = 0
    WAITING = 1
    ABORTED = 2
    COMMITTED = 3
