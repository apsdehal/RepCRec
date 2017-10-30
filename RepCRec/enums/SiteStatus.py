from enum import Enum


class ServerStatus(Enum):
    UP = 0
    DOWN = 1
    RECOVERING = 2
