import os

from .Instruction import Instruction


class IO:
    def __init__(self, file_name):
        self.file_name = file_name
        self.line_generator = self._get_line_generator()

    def get_next_instruction(self):
        line = next(self.line_generator, None)

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
            instructions.append(Instruction(instruction))

    def print_all_variables(self, data_table):
        return None

    def print_variable(self, variable):
        return None

    def print_site(self, site):
        return None
