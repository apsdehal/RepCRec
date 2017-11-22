import os
from .Instruction import Instruction
from .Variable import Variable


class IO:

    BEGIN_FUNC = "begin"
    BEGIN_READ_ONLY_FUNC = "beginRO"
    READ_FUNC = "R"
    WRITE_FUNC = "W"
    DUMP_FUNC = "dump"
    END_FUNC = "end"
    FAIL_FUNC = "fail"
    RECOVER_FUNC = "recover"

    def __init__(self, file_name, site_manager, transaction_manager):
        self.file_name = file_name
        self.line_generator = self._get_line_generator()
        self.site_manager = site_manager
        self.transaction_manager = transaction_manager

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

    def run(self):
        instruction = self.get_next_instruction()

        while instruction != None:

            if instruction.get_instruction_type() == self.BEGIN_FUNC:
                self.transaction_manager.begin(instruction.get_params())

            elif instruction.get_instruction_type() ==
                 self.BEGIN_READ_ONLY_FUNC:

                self.transaction_manager.begin_read_only(
                    instruction.get_params())

            elif instruction.get_instruction_type() == self.WRITE_FUNC:
                params = instruction.get_params()

                if int(params[1][1:]) % 2 == 0:
                    self.transaction_manager.write_request_even(params)
                else:
                    self.transaction_manager.write_request(params)

            elif(instruction.get_instruction_type() == self.DUMP_FUNC):
                # self.dump(instruction.get_params())

                params = instruction.get_params()

                if len(params) == 0:
                    for site in self.site_manager.sites:
                        site.dump_site()

                elif len(params[0]) == 1:

                    site = self.site_manager.get_site(int(params[0]))
                    site.dump_site()

                elif len(params[0]) == 2:

                    sites = Variable.get_site(int(params[0][1:]))

                    if sites == 'all':
                        for site in self.site_manager.sites:
                            variables = site.get_all_variables()

                            for variable in variables:
                                if variable.name == params[0]:
                                    print(variable.value)
                    else:
                        site = self.site_manager.get_site(int(params[0]))

                        variables = site.get_all_variables()

                            for variable in variables:
                                if variable.name == params[0]:
                                    print(variable.value)


            elif(instruction.get_instruction_type() == self.FAIL_FUNC):
                self.site_manager.fail(int(instruction.get_params()[0]))

            elif(instruction.get_instruction_type() == self.RECOVER_FUNC):
                self.site_manager.recover(int(instruction.get_params()[0]))

            elif(instruction.get_instruction_type() == self.END_FUNC):
                # TODO
                # self.site_manager.fail(int(instruction.get_params()[0]))

            # self.transaction_manager.tick(instruction)
            # instruction = self.io.get_next_instruction()
