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

    def commit_transaction(self, name):

        uncommited_variables = \
            self.transaction_map[name].get_uncommitted_variables()

        for variable, value in uncommited_variables.items():

            for i in range(1, self.number_of_sites + 1):

                if variable % 2 == 0 or ((variable % 10) + 1) == i:

                    site = self.site_manager.get_site(i)
                    site.DataManager.variable_map[
                        variable].value = value
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

        self.transaction_map[params[0]] = Transaction(
            int(params[0][1:]), params[0])

        self.transaction_queue.append(self.transaction_map[params[0]].id)
        return

    def begin_read_only(self, params):
        self.transaction_map[params[0]] = Transaction(
            int(params[0][1:]), params[0], True)
        return

    # def write_request(self, id, variable, value):
    def write_request(self, params):

        t_id = params[0]
        variable = int(params[1][1:])
        value = int(params[2])

        if self.transaction_map[t_id].get_status() != \
                TransactionStatus.RUNNING:
            return

        if self.lock_table.is_locked(variable):

            if self.transaction_queue.index(self.lock_table.lock_map[variable].transaction.id) \
                    < self.transaction_queue.index(int(t_id[1:])):
                self.transaction_map[t_id].set_status(
                    TransactionStatus.ABORTED)

            else:

                self.transaction_map[t_id].set_status(
                    TransactionStatus.WAITING)

                if variable not in self.lock_table.lock_queue:
                    self.lock_table.lock_queue[variable] = list()

                self.lock_table.lock_queue[variable].append(t_id)

        else:

            lock = Lock(LockType.WRITE, self.transaction_map[params[0]])
            self.lock_table.lock_map[variable] = lock

            self.transaction_map[t_id].uncommitted_variables[variable] = value
            # for i in range(1, num_sites + 1):

            #     if variable % 2 == 0 or ((variable % 10) + 1) == i:

            #         site = self.site_manager.get_site(i)
            #         site.DataManager.variable_map[variable].value = value

        return

    def write_request_even(self, params):

        return

    def end(self, num, name):
        return

    def read_request(self, id, variable):
        return

    def fail(self):
        return

    def recover(self):
        return
