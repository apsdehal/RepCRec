# Revise this module again according to design doc
from .Transaction import Transaction
from .Lock import Lock
from .LockTable import LockTable

from .enums.LockType import LockType
from .enums.TransactionStatus import TransactionStatus


class TransactionManager:

    BEGIN_FUNC = "begin"
    BEGIN_READ_ONLY_FUNC = "beginRO"
    READ_FUNC = "R"
    WRITE_FUNC = "W"
    DUMP_FUNC = "dump"
    END_FUNC = "end"
    FAIL_FUNC = "fail"
    RECOVER_FUNC = "recover"

    def __init__(self, num_vars, num_sites, lock_table, site_manager):
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.current_time = 0
        self.lock_table = lock_table
        self.transaction_queue = list()
        self.site_manager = site_manager
        self.waiting_transactions = dict()

    def commit_transaction(self, name):

        uncommited_variables = \
            self.transaction_map[name].get_uncommitted_variables

        for variable, value in self.transaction_map.items():

            for i in range(1, self.number_of_sites + 1):

                if int(variable[1:]) % 2 == 0 or ((int(variable[1:]) % 10) + 1) == i:

                    site = self.site_manager.get_site(i)
                    site.DataManager.variable_map[
                        int(variable[1:])].value = value
        return

    def tick(self, instruction):

        if instruction.get_instruction_type() == self.BEGIN_FUNC:
            self.begin(instruction.get_params())

        elif instruction.get_instruction_type() == self.BEGIN_READ_ONLY_FUNC:
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
        current_index = len(self.transaction_map)

        self.transaction_map[params[0]] = Transaction(
            current_index, params[0])

    def begin_read_only(self, params):
        current_index = len(self.transaction_map)

        self.transaction_map[params[0]] = Transaction(
            current_index, params[0], True)

    def write_request(self, params):

        transaction_name = params[0]
        variable = params[1]
        value = int(params[2])
        transaction = self.lock_table.lock_map[variable].transaction.name

        if transaction.get_status() != \
                TransactionStatus.RUNNING:
            return

        if self.site_manager.get_lock(transaction,
                                      LockType.WRITE, variable):
            self.lock_table.set_lock(transaction,
                                     LockType.WRITE, variable)
            transaction.uncommitted_variables[variable] = value
        else:
            transaction.set_status(TransactionStatus.WAITING)
            lock = self.lock_table.lock_map[variable]
            blocking_transaction = lock.transaction.name
            blocking_transaction = (variable, blocking_transaction)
            self.waiting_transactions[transaction_name] = blocking_transaction

    def end(self, num, name):
        return

    def read_request(self, id, variable):
        return

    def fail(self):
        return

    def recover(self):
        return
