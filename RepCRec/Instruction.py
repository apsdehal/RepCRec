import re


class Instruction:
    PARAM_MATCHER = "\((.*?)\)"

    def __init__(self, instruction):
        self.instruction_type = instruction.split('(')[0]
        self.params = re.search(self.PARAM_MATCHER, instruction).group()
        self.params.strip('()')
        self.params = map(lambda x: x.strip(), self.params.split(','))

    def get_params(self):
        return self.params

    def get_instruction_type(self):
        return self.instruction_type
