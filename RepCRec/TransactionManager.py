# Revise this module again according to design doc
from .Transaction import Transaction
from .Lock import Lock


class TransactionManager:

    BEGIN_FUNC = "begin"
    BEGIN_READ_ONLY_FUNC = "beginRO"
    READ_FUNC = "R"
    WRITE_FUNC = "W"
    DUMP_FUNC = "dump"
    END_FUNC = "end"
    FAIL_FUNC = "fail"
    RECOVER_FUNC = "recover"

    def __init__(self, num_vars, num_sites):
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.current_time = 0

    def commit_transaction(self, name):
        return

    def tick(self, instruction):

        if(instruction.get_instruction_type() == self.BEGIN_FUNC):
            self.begin(instruction.get_params())

        elif(instruction.get_instruction_type() == self.BEGIN_READ_ONLY_FUNC):
            self.begin_read_only(instruction.get_params())

        elif(instruction.get_instruction_type() == self.WRITE_FUNC):
            params = instruction.get_params()

            if int(params[1][1:]) % 2 == 0:
                self.write_request_even(params)
            else:
                self.write_request(params)

        elif(instruction.get_instruction_type() == self.DUMP_FUNC):
            self.dump(instruction.get_params())

        elif(instruction.get_instruction_type() == self.FAIL_FUNC):
            self.fail(instruction.get_params())

        elif(instruction.get_instruction_type() == self.RECOVER_FUNC):
            self.recover(instruction.get_params())

        elif(instruction.get_instruction_type() == self.END_FUNC):
            self.end(instruction.get_params())

        return

    # def begin(self, name):
    def begin(self, params):

        self.transaction_map[params[0]] = Transaction(
            int(params[0][1:]), params[0])
        return

    def begin_read_only(self, params):
        self.transaction_map[params[0]] = Transaction(
            int(params[0][1:]), params[0], True)
        return

    # def write_request(self, id, variable, value):
    def write_request(self, params):

        lock = Lock("write", transaction_map[params[0]])

        return

    def write_request_even(self, transaction, variable, value):
        return

    def end(self, num, name):
        return

    def read_request(self, id, variable):
        return

    def fail(self):
        return

    def recover(self):
        return
