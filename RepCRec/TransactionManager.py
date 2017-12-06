# Revise this module again according to design doc
import logging

from .Transaction import Transaction
from .Lock import Lock
from .LockTable import LockTable

from .enums.LockAcquireStatus import LockAcquireStatus
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
        # self.variable_values = dict()

    def tick(self, instruction):
        self.clear_aborted()
        self.detect_and_clear_deadlocks()
        self.blocked_to_waiting()
        self.try_waiting()

        params = list(instruction.get_params())

        # log.info(params, instruction.get_instruction_type())

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
            self.end(params)
        else:
            log.info("We have a problem")

    def begin(self, params):

        current_index = len(self.transaction_map)

        log.info("Starting " + params[0])
        self.transaction_map[str(params[0])] = Transaction(
            current_index, params[0])

    def begin_read_only(self, params):

        current_index = len(self.transaction_map)

        log.info("Starting read only transaction " + params[0])
        self.transaction_map[params[0]] = Transaction(
            current_index, params[0], True)

        self.transaction_map[
            params[0]].variable_values = self.site_manager.get_current_variables()

    def write_request(self, params):
        transaction_name = params[0]
        variable = params[1]
        value = int(params[2])

        if transaction_name not in self.transaction_map:
            return

        transaction = self.transaction_map[transaction_name]

        is_waiting = transaction.get_status() == TransactionStatus.WAITING
        is_running = transaction.get_status() == TransactionStatus.RUNNING

        if not (is_waiting or is_running):
            return
        if self.lock_table.is_locked_by_transaction(transaction, variable, LockType.WRITE):

            log.info(transaction.name +
                     " already has a write lock on " + variable)
            transaction.uncommitted_variables[variable] = value
            transaction.set_status(TransactionStatus.RUNNING)
            return

        lock_acquire_status = self.site_manager.get_locks(transaction,
                                                          LockType.WRITE,
                                                          variable)
        if lock_acquire_status == LockAcquireStatus.GOT_LOCK:

            log.info(transaction.name + " got write lock on " + variable)
            self.lock_table.set_lock(transaction,
                                     LockType.WRITE, variable)

            transaction.uncommitted_variables[variable] = value
            transaction.set_status(TransactionStatus.RUNNING)

        elif lock_acquire_status == LockAcquireStatus.ALL_SITES_DOWN:

            log.info(transaction.name + " is waiting on " + variable)
            waiting_txn_tuple = (InstructionType.WRITE,
                                 variable,
                                 value)
            transaction.set_status(TransactionStatus.WAITING)
            self.waiting_transactions[transaction.name] = waiting_txn_tuple

        else:

            for lock in self.lock_table.lock_map[variable]:

                blocking_transaction = lock.transaction.name

                if lock.transaction == transaction:
                    continue

                blocking_txn_tuple = (blocking_transaction,
                                      InstructionType.WRITE,
                                      variable,
                                      value)
                log.info(transaction.name + " is blocked by " +
                         blocking_transaction + " on " + variable)
                transaction.set_status(TransactionStatus.BLOCKED)

                if transaction_name not in self.blocked_transactions:
                    self.blocked_transactions[transaction_name] = []

                self.blocked_transactions[
                    transaction_name].append(blocking_txn_tuple)

    def read_request_read_only(self, transaction, variable, transaction_name):

        if variable in transaction.variable_values:
            transaction.read_variables[
                variable] = transaction.variable_values[variable]

        else:

            transaction.set_status(TransactionStatus.WAITING)
            waiting_txn = (transaction,
                           InstructionType.READ,
                           variable)
            self.waiting_transactions[transaction_name] = waiting_txn

        return

    def read_request(self, params):

        transaction_name = params[0]
        variable = params[1]

        if transaction_name not in self.transaction_map:
            return

        transaction = self.transaction_map[transaction_name]

        is_waiting = transaction.get_status() == TransactionStatus.WAITING
        is_running = transaction.get_status() == TransactionStatus.RUNNING

        if not (is_waiting or is_running):
            return

        if transaction.is_read_only:
            self.read_request_read_only(
                transaction, variable, transaction_name)

        else:

            if self.lock_table.is_locked_by_transaction(transaction, variable, LockType.READ) or \
                    self.lock_table.is_locked_by_transaction(transaction, variable, LockType.WRITE):

                log.info(transaction.name +
                         " already has a read lock on " + variable)
                transaction.set_status(TransactionStatus.RUNNING)
                return

            lock_acquire_status = self.site_manager.get_locks(transaction,
                                                              LockType.READ,
                                                              variable)

            if lock_acquire_status == LockAcquireStatus.GOT_LOCK or lock_acquire_status == LockAcquireStatus.GOT_LOCK_RECOVERING:

                if lock_acquire_status == LockAcquireStatus.GOT_LOCK:

                    log.info(transaction.name + " got read lock on " + variable +
                             " having value " + str(self.site_manager.get_current_variables(variable)))

                else:

                    log.info("Although, the site holding " + variable + " is recovering, " + transaction.name + " got read lock on " +
                             variable + " having value " + str(self.site_manager.get_current_variables(variable)) + " since its the only copy")

                transaction.read_variables[
                    variable] = self.site_manager.get_current_variables(variable)

                self.lock_table.set_lock(transaction,
                                         LockType.READ, variable)

                transaction.set_status(TransactionStatus.RUNNING)

            elif lock_acquire_status == LockAcquireStatus.ALL_SITES_DOWN:

                log.info(transaction.name + " is waiting on " + variable)
                waiting_txn_tuple = (InstructionType.READ, variable)

                transaction.set_status(TransactionStatus.WAITING)
                self.waiting_transactions[transaction.name] = waiting_txn_tuple

            else:
                for lock in self.lock_table.lock_map[variable]:
                    blocking_transaction = lock.transaction.name

                    if lock.transaction == transaction:
                        continue

                    blocking_txn_tuple = (blocking_transaction,
                                          InstructionType.READ,
                                          variable)

                    log.info(transaction.name + " is blocked by " +
                             blocking_transaction + " on " + variable)
                    transaction.set_status(TransactionStatus.BLOCKED)

                    if transaction_name not in self.blocked_transactions:
                        self.blocked_transactions[transaction_name] = []

                    self.blocked_transactions[
                        transaction_name].append(blocking_txn_tuple)

        return

    def clear_aborted(self):

        to_pop = list()

        for trn_name, transaction in self.transaction_map.items():

            if transaction.get_status() == TransactionStatus.ABORTED:

                to_pop.append(trn_name)
                self.abort(trn_name)

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

            for block in self.blocked_transactions[transaction]:
                block = block[0]

                if self.transaction_map[block].get_status() == TransactionStatus.ABORTED:
                    continue
                if block in visited:
                    return visited[block]
                else:
                    return self.detect_deadlock(block, visited, current)
            return 0
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

        log.info(max_name + " aborted as it is youngest in a deadlock")
        self.abort(max_name)

    def blocked_to_waiting(self):

        to_pop = list()

        for key, block in self.blocked_transactions.items():
            is_clear = True
            for blocking_transaction in block:
                blocking_transaction = self.transaction_map[
                    blocking_transaction[0]]
                is_aborted = blocking_transaction.get_status() == TransactionStatus.ABORTED
                is_committed = blocking_transaction.get_status() == TransactionStatus.COMMITTED
                is_clear &= is_aborted or is_committed

            if is_clear:
                self.waiting_transactions[key] = block[0][1:]
                transaction = self.transaction_map[key]
                transaction.set_status(TransactionStatus.WAITING)
                to_pop.append(key)
        for key in to_pop:
            self.blocked_transactions.pop(key)

    def abort(self, name):
        if name in self.blocked_transactions:
            self.blocked_transactions.pop(name)

        if name in self.waiting_transactions:
            self.waiting_transactions.pop(name)

        transaction = self.transaction_map[name]
        transaction.set_status(TransactionStatus.ABORTED)
        self.clear_locks(transaction)

    def clear_locks(self, transaction):
        for var_name in list(self.lock_table.get_lock_map()):
            locks = self.lock_table.get_lock_map()[var_name]
            for lock in locks:
                if lock.transaction == transaction:

                    log.info("Clearing locks for " + transaction.name +
                             " variable: " + var_name)
                    self.site_manager.clear_locks(lock, var_name)
                    self.lock_table.clear_lock(lock, var_name)

    def try_waiting(self):
        to_pop = list()
        for transaction in self.waiting_transactions:
            params = self.waiting_transactions[transaction]
            transaction_obj = self.transaction_map[transaction]

            if params[0] == InstructionType.WRITE:
                self.write_request((transaction, params[1], params[2]))
            elif params[0] == InstructionType.READ:
                self.read_request((transaction, params[1]))

            if self.transaction_map[transaction].get_status() == \
               TransactionStatus.RUNNING:
                to_pop.append(transaction)

        for transaction in to_pop:
            self.waiting_transactions.pop(transaction)

    def commit_transaction(self, name):
        status = self.transaction_map[name].get_status()
        if status == TransactionStatus.COMMITTED or status == TransactionStatus.ABORTED:
            return

        transaction = self.transaction_map[name]
        read_variables = transaction.get_read_variables()

        for variable, value in read_variables.items():
            log.info(name + " read the value " + str(value) +
                     " of variable " + variable)

        uncommited_variables = transaction.get_uncommitted_variables()

        for variable, value in uncommited_variables.items():

            for i in range(1, self.number_of_sites + 1):
                var = int(variable[1:])
                if var % 2 == 0 or ((var % 10) + 1) == i:
                    site = self.site_manager.get_site(i)
                    site.write_variable(transaction,
                                        variable,
                                        value)
        self.transaction_map[name].set_status(TransactionStatus.COMMITTED)

    def end(self, params):
        status = self.transaction_map[params[0]].get_status()
        if status == TransactionStatus.COMMITTED or status == TransactionStatus.ABORTED:
            return

        self.commit_transaction(params[0])
        log.info(params[0] + " committed")
        self.clear_locks(self.transaction_map[params[0]])

    def fail(self):
        return

    def recover(self):
        return
