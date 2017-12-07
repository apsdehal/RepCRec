"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from enum import Enum


class InstructionType(Enum):
    READ = 0
    READ_ONLY = 1
    WRITE = 2
