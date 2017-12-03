# Revise this module again according to design doc
import logging

from .Transaction import Transaction
from .Lock import Lock
from .LockTable import LockTable

from .enums.LockType import LockType
from .enums.TransactionStatus import TransactionStatus
from .enums.InstructionType import InstructionType
from .constants import BEGIN_FUNC, BEGIN_READ_ONLY_FUNC, WRITE_FUNC, \
    READ_FUNC, END_FUNC

log = logging.getLogger(__name__)


class TransactionManager:

    def __init__(self, num_vars, num_sites, lock_table, site_manager):
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.current_time = 0
        self.lock_table = lock_table
        self.transaction_queue = list()
        self.site_manager = site_manager
        self.blocked_transactions = dict()
        self.waiting_transactions = dict()

    def commit_transaction(self, name):
        transaction = self.transaction_map[name]
        uncommited_variables = transaction.get_uncommitted_variables()

        for variable, value in uncommited_variables.items():

            for i in range(1, self.number_of_sites + 1):
                var = int(variable[1:])
                if var % 2 == 0 or ((var % 10) + 1) == i:
                    site = self.site_manager.get_site(i)
                    site.data_manager.write_variable(transaction,
                                                     variable,
                                                     value)

    def tick(self, instruction):
        self.blocked_transactions = dict()
        self.detect_and_clear_deadlocks()
        self.blocked_to_waiting()
        self.try_waiting()

        params = list(instruction.get_params())

        if instruction.get_instruction_type() == BEGIN_FUNC:
            self.begin(params)

        elif instruction.get_instruction_type() == \
                BEGIN_READ_ONLY_FUNC:
            self.begin_read_only(params)

        elif instruction.get_instruction_type() == READ_FUNC:
            self.read_request(params)

        elif instruction.get_instruction_type() == WRITE_FUNC:
            self.write_request(params)

        elif instruction.get_instruction_type() == END_FUNC:
            self.commit_transaction(params[0])

    def begin(self, params):
        current_index = len(self.transaction_map)

        self.transaction_map[params[0]] = Transaction(
            current_index, params[0])

    def begin_read_only(self, params):
        current_index = len(self.transaction_map)

        self.transaction_map[params[0]] = Transaction(
            current_index, params[0], True)

        self.variable_values = self.site_manager.get_current_variables()

    def write_request(self, params):
        transaction_name = params[0]
        variable = params[1]
        value = int(params[2])
        transaction = self.transaction_map[transaction_name]

        if transaction.get_status() != \
                TransactionStatus.RUNNING:
            return

        if self.site_manager.get_locks(transaction,
                                       LockType.WRITE, variable):
            print(variable)
            self.lock_table.set_lock(transaction,
                                     LockType.WRITE, variable)
            transaction.uncommitted_variables[variable] = value
            transaction.set_status(TransactionStatus.RUNNING)
        else:

            transaction.set_status(TransactionStatus.WAITING)
            print(variable)
            lock = self.lock_table.lock_map[variable]
            blocking_transaction = lock.transaction.name
            blocking_transaction = (blocking_transaction,
                                    InstructionType.WRITE,
                                    variable,
                                    value)
            transaction.set_status(TransactionStatus.BLOCKED)
            self.blocked_transactions[transaction_name] = blocking_transaction

    def detect_and_clear_deadlocks(self):
        for x in list(self.blocked_transactions):
            visited = dict()
            current = []
            index = self.detect_deadlock(x, visited, current)

            if index != 0:
                self.clear_deadlock(current, index - 1)

    def detect_deadlock(self, transaction, visited, current):
        if transaction in self.blocked_transactions:
            visited[transaction] = len(current) + 1
            current.append(transaction)

            block = self.blocked_transactions[transaction][0]

            if block in visited:
                return visited[block]
            else:
                return self.detect_deadlock(block, visited, current)
        else:
            return 0

    def clear_deadlock(self, transaction_list, index):
        transaction_list = transaction_list[index:]

        max_id = -1
        max_name = None

        for name in transaction_list:
            if max_id < self.transaction_map[name].id:
                max_id = self.transaction_map[name].id
                max_name = name

        self.abort(max_name)

    def blocked_to_waiting(self):
        for key, block in self.blocked_transactions.items():
            if block[0] not in self.transaction_map:
                self.waiting_transactions[block[0]] = (block[1],
                                                       block[2],
                                                       block[3])
                transaction = self.transaction_map[block[0]]
                transaction.set_status(TransactionStatus.WAITING)
                self.blocked_transactions.pop(key)

    def abort(self, name):
        self.blocked_transactions.pop(name)
        transaction = self.transaction_map.pop(name)
        self.clear_locks(transaction)

    def clear_locks(self, transaction):
        for var_name in list(self.lock_table.get_lock_map()):
            lock = self.lock_table.get_lock_map()[var_name]
            if lock.transaction == transaction:
                self.site_manager.clear_locks(lock, var_name)
                self.lock_table.free(var_name)

    def try_waiting(self):
        for transaction in self.waiting_transactions:
            params = self.waiting_transactions[transaction]
            if params[0] == InstructionType.WRITE:
                self.write_request((transaction, params[1], params[2]))
            elif params[0] == InstructionType.READ:
                self.read_request((transaction, params[1], params[2]))
            elif params[0] == InstructionType.READ_ONLY:
                self.read_only_request((transaction, params[1], params[2]))

            if self.transaction_map[transaction].get_status() == \
               TransactionStatus.RUNNING:
                self.waiting_transactions.pop(transaction)

    def end(self, num, name):
        return

    def read_request(self, params):

        transaction_name = params[0]
        variable = params[1]

        transaction = self.lock_table.lock_map[variable].transaction

        if transaction.get_status() != \
                TransactionStatus.RUNNING:
            return

        if transaction.is_read_only():

            if variable in self.variable_values:
                log.debug(str(transaction.name) + " reads " + variable +
                          " having value " + self.variable_values[variable])

            else:

                transaction.set_status(TransactionStatus.WAITING)

        if self.site_manager.get_locks(transaction, LockType.READ, variable):

            self.lock_table.set_lock(transaction, LockType.READ, variable)

            log.debug(str(transaction.name) + " reads " + variable)

        return

    def fail(self):
        return

    def recover(self):
        return
