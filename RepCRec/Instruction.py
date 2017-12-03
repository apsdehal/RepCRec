import re


class Instruction:
    PARAM_MATCHER = "\((.*?)\)"

    def __init__(self, instruction):

        self.instruction_type = instruction.split('(')[0]
        self.instruction_type = self.instruction_type.strip(" ")

        self.params = re.search(self.PARAM_MATCHER, instruction).group()
        # print(type(self.params))
        self.params = self.params.strip('()')
        self.params = map(lambda x: x.strip(), self.params.split(','))
        # print(self.params)

    def get_params(self):
        return self.params

    def get_instruction_type(self):
        return self.instruction_type
