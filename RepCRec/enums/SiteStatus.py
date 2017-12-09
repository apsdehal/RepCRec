"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class SiteStatus(Enum):
    """
    Tells status of a particular site
    """
    UP = 0
    DOWN = 1
    RECOVERING = 2
