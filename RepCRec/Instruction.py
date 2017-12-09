"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
import re


class Instruction:
    """
    This class represents an instruction from the ipput file

    Args:
        instruction: Raw string which is to properly processed
    """
    PARAM_MATCHER = "\((.*?)\)"

    def __init__(self, instruction):

        self.instruction_type = instruction.split('(')[0]
        self.instruction_type = self.instruction_type.strip(" ")

        self.params = re.search(self.PARAM_MATCHER, instruction).group()
        self.params = self.params.strip('()')
        self.params = map(lambda x: x.strip(), self.params.split(','))

    def get_params(self):
        """
        Get params of this instruction

        Returns:
            List of params of this instruction
        """
        return self.params

    def get_instruction_type(self):
        """
        Get instruction type of this instruction

        Returns:
            Instruction type of the instruction
        """
        return self.instruction_type
