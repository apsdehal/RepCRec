"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
import os
from .Instruction import Instruction
from .Variable import Variable
from .constants import SITE_MANAGER_FUNCS


class IO:

    def __init__(self, file_name, site_manager, transaction_manager,
                 lock_table, stdin=False):
        # print("Hello")

        self.file_name = file_name
        self.stdin = stdin
        if not self.stdin:
            self.line_generator = self._get_line_generator()
        self.site_manager = site_manager
        self.transaction_manager = transaction_manager
        self.lock_table = lock_table

    def get_next_instruction(self):
        if not self.stdin:
            line = next(self.line_generator, None)
        else:
            line = input().strip()
        # print(line)

        if line is None:
            return line
        else:
            return self._process_instruction(line)

    def _get_line_generator(self):
        with open(self.file_name, 'r') as input_file:
            for line in input_file:
                if len(line) > 1:
                    yield line

    def _process_instruction(self, line):
        line = line.strip().split(";")
        instructions = []
        for instruction in line:
            if instruction.find("//") == 0:
                # Remove comments
                continue
            instructions.append(Instruction(instruction))

        return instructions

    def run(self):

        instructions = self.get_next_instruction()

        while instructions is not None:

            for instruction in instructions:

                if instruction.get_instruction_type() in SITE_MANAGER_FUNCS:
                    self.site_manager.tick(instruction)
                else:
                    self.transaction_manager.tick(instruction)

            instructions = self.get_next_instruction()
