# Revise this module again according to design doc
from .Transaction import Transaction
from .Lock import Lock
from .LockTable import LockTable

from .enums.LockType import LockType
from .enums.TransactionStatus import TransactionStatus
from .constants import BEGIN_FUNC, BEGIN_READ_ONLY_FUNC, WRITE_FUNC, READ_FUNC


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
        self.blocked_transactions = dict()

    def commit_transaction(self, name):
        transaction = self.transaction_map[name]
        uncommited_variables = transaction.get_uncommitted_variables()

        for variable, value in self.transaction_map.items():

            for i in range(1, self.number_of_sites + 1):
                var = int(variable[1:])
                if var % 2 == 0 or ((var % 10) + 1) == i:
                    site = self.site_manager.get_site(i)
                    site.data_manager.write_variable(transaction,
                                                     variable,
                                                     value)

    def tick(self, instruction):
        self.detect_and_clear_deadlocks()
        if instruction.get_instruction_type() == BEGIN_FUNC:
            self.transaction_manager.begin(params)

        elif instruction.get_instruction_type() == \
                BEGIN_READ_ONLY_FUNC:
            self.transaction_manager.begin_read_only(
                params)

        elif instruction.get_instruction_type() == WRITE_FUNC:
            self.transaction_manager.write_request(params)

        elif instruction.get_instruction_type() == END_FUNC:
            self.transaction_manager.commit_transaction(params[0])

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
            self.blocked_transactions[transaction_name] = blocking_transaction

    def detect_and_clear_deadlocks(self):
        for x in self.blocked_transactions:
            visited = dict()
            current = []
            index = detect_deadlock(x, visited, current)
            self.clear_deadlock(current, index)

    def detect_deadlock(self, transaction, visited, current):
        if transaction in self.blocked_transactions:
            visited[transaction] = len(current) + 1
            current.append(transaction)

            block = self.blocked_transactions[transaction][0]

            if block in visited:
                return visited[block]
            else:
                return self.detect_deadlock(block, current)
        else:
            return 0

    def clear_deadlock(self, transaction_list, index):
        transaction_name = transaction_list[index]
        self.abort(transaction_name)

    def abort(self, name):
        self.blocked_list.pop(name)
        transaction = self.transaction_map.pop(name)
        self.clear_locks(transaction)

    def clear_locks(self, transaction):
        for var_name, lock in self.lock_table.get_lock_map().items():
            if lock.transaction == transaction:
                self.site_manager.clear_locks(lock, var_name)
                self.lock_table.free(int(var_name[1:]))

    def end(self, num, name):
        return

    def read_request(self, id, variable):
        return

    def fail(self):
        return

    def recover(self):
        return
