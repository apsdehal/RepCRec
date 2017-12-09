"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class InstructionType(Enum):
    """
    Type of instruction present
    """
    READ = 0
    READ_ONLY = 1
    WRITE = 2
