"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
import logging
from collections import defaultdict

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
    """
    Transaction manager class is reponsible for processing transactions
    and deadlock detection and resolution. It is also responsible for
    calling site manager to clear locks when a transaction commits.

    Attributes:
        num_vars (int): Number of variables
        num_sites (int): Number of sites
        lock_table (class object): Global instance of LockTable
        site_manager (class object): Global instance of SiteManager
        transaction_map (dict): Maps transaction name to Transaction class
                                object
        blocked_transactions (dict): stores blocked transaction name mapped to
                                     blocking transaction tuple
                                     (transaction name, lock type, variable, .)
        waiting_transaction (dict): stores waiting transaction name mapped to
                                    a tuple containing relevant information
    """

    def __init__(self, num_vars, num_sites, lock_table, site_manager):
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.lock_table = lock_table
        self.site_manager = site_manager
        self.current_time = 0
        self.blocked_transactions = defaultdict(dict)
        self.waiting_transactions = defaultdict(dict)

    def tick(self, instruction):
        """

        Method responsible for calling other methods based on instruction type.
        Also checks for deadlocks on every tick

        Args:
            instruction : object of class Instruction, contains the
                          current instruction

        """
        self.current_time += 1
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
        """
        Method responsible for initializing a transaction and making a new
        instance of Transaction class

        Args:
            params : list of parameters of the parsed instruction, containing
                     instruction name

        """

        current_index = len(self.transaction_map)

        log.info("Starting " + params[0])
        self.transaction_map[str(params[0])] = Transaction(
            current_index, params[0])

        return

    def begin_read_only(self, params):
        """
        Method responsible for initializing a read only transaction and making
        a new instance of Transaction class

        Args:
            params : list of parameters of the parsed instruction, containing
                     instruction name

        """

        current_index = len(self.transaction_map)

        log.info("Starting read only transaction " + params[0])
        self.transaction_map[params[0]] = Transaction(
            current_index, params[0], True)

        self.transaction_map[
            params[0]].variable_values = \
            self.site_manager.get_current_variables()

    def write_request(self, params):
        """
        Method responsible for processing a write request, gets write locks on
        the variable to be written,
        in case it is not able to get locks, changes status to blocked or
        waiting according to situation.
        Also inserts transactions in waiting or blocked transaction dicts
        as required.

        Args:
            params : list of parameters of the parsed instruction, containing
                     instruction name

        """

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
        if self.lock_table.is_locked_by_transaction(transaction, variable,
                                                    LockType.WRITE):

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
            self.waiting_transactions[self.current_time][
                transaction.name] = waiting_txn_tuple

        else:

            for lock in self.lock_table.lock_map[variable]:

                blocking_transaction = lock.transaction.name

                if lock.transaction == transaction:
                    continue

                blocking_txn_tuple = (blocking_transaction,
                                      InstructionType.WRITE,
                                      variable,
                                      value)
                log.info(transaction.name +
                         " is blocked for a write lock by " +
                         blocking_transaction + " on " + variable)
                transaction.set_status(TransactionStatus.BLOCKED)

                self.current_time += 1
                # if transaction_name not in self.blocked_transactions:
                #     self.blocked_transactions[transaction_name] = []

                self.blocked_transactions[self.current_time][
                    transaction_name] = blocking_txn_tuple

    def read_request_read_only(self, transaction, variable, transaction_name,
                               try_waiting):
        """
        Method responsible for processing a read request from a r
        ead only transaction,
        if the site holding the variable is down, it waits for the site to
        be up and then reads the variable.

        Args:
            params : list of parameters of the parsed instruction, containing
                     instruction name

        """
        if try_waiting:

            val = self.site_manager.get_current_variables(variable)

            if val is None:
                transaction.set_status(TransactionStatus.RUNNING)
                transaction.variable_values[variable] = val
            else:
                return

        if variable in transaction.variable_values:

            if variable not in transaction.read_variables:
                transaction.read_variables[variable] = list()

            transaction.read_variables[
                variable].append(transaction.variable_values[variable])

        else:

            transaction.set_status(TransactionStatus.WAITING)
            log.info(transaction.name + " is waiting on " + variable)
            waiting_txn = (InstructionType.READ_ONLY, variable)
            self.waiting_transactions[self.current_time][
                transaction_name] = waiting_txn

        return

    def read_request(self, params, try_waiting=False):

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
                transaction, variable, transaction_name, try_waiting)

        else:

            if self.lock_table.is_locked_by_transaction(transaction, variable,
                                                        LockType.READ) or \
                    self.lock_table.is_locked_by_transaction(transaction,
                                                             variable,
                                                             LockType.WRITE):

                log.info(transaction.name +
                         " already has a read lock on " + variable)
                transaction.set_status(TransactionStatus.RUNNING)
                return

            for blocked_tuple_dict in self.blocked_transactions.values():

                for key, blocked_tuple in blocked_tuple_dict.items():

                    # print(len(blocked_tuple), variable)

                    if len(blocked_tuple) == 4 and \
                            blocked_tuple[2] == variable:

                        for lock in self.lock_table.lock_map[variable]:

                            blocking_transaction = lock.transaction.name

                            if lock.transaction == transaction:
                                continue

                            blocking_txn_tuple = (blocking_transaction,
                                                  InstructionType.READ,
                                                  variable)

                            transaction.set_status(TransactionStatus.BLOCKED)

                            self.blocked_transactions[self.current_time][
                                transaction_name] = blocking_txn_tuple

                            log.info(transaction_name + " will not get a " +
                                     "read lock on " + variable +
                                     " because " + key +
                                     " is already waiting for a write lock")

                            return

            lock_acquire_status = self.site_manager.get_locks(
                transaction, LockType.READ, variable)

            if lock_acquire_status == LockAcquireStatus.GOT_LOCK or \
                    lock_acquire_status == \
                    LockAcquireStatus.GOT_LOCK_RECOVERING:

                if lock_acquire_status == LockAcquireStatus.GOT_LOCK:

                    log.info(transaction.name + " got read lock on " +
                             variable + " having value " +
                             str(self.site_manager.get_current_variables(
                                 variable)))

                else:

                    log.info("Although, the site holding " + variable +
                             " is recovering, " + transaction.name +
                             " got read lock on " +
                             variable + " having value " +
                             str(self.site_manager.get_current_variables(
                                 variable)) + " since its the only copy")

                if variable not in transaction.read_variables:
                    transaction.read_variables[variable] = list()

                transaction.read_variables[
                    variable] = \
                    self.site_manager.get_current_variables(variable)

                self.lock_table.set_lock(transaction,
                                         LockType.READ, variable)

                transaction.set_status(TransactionStatus.RUNNING)

            elif lock_acquire_status == LockAcquireStatus.ALL_SITES_DOWN:

                log.info(transaction.name + " is waiting on " + variable)
                waiting_txn_tuple = (InstructionType.READ, variable)

                transaction.set_status(TransactionStatus.WAITING)
                self.waiting_transactions[self.current_time][
                    transaction.name] = waiting_txn_tuple

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

                    self.current_time += 1

                    # if transaction_name not in self.blocked_transactions:
                    #     self.blocked_transactions[transaction_name] = []

                    self.blocked_transactions[self.current_time][
                        transaction_name] = blocking_txn_tuple

        return

    def clear_aborted(self):

        to_pop = list()

        for trn_name, transaction in self.transaction_map.items():

            if transaction.get_status() == TransactionStatus.ABORTED:

                to_pop.append(trn_name)
                self.abort(trn_name)

    def detect_and_clear_deadlocks(self):

        squashed_blocked_transactions = \
            self.get_squashed_blocked_transactions()

        for x in list(squashed_blocked_transactions):

            visited = dict()
            current = []
            self.detect_deadlock(
                x, visited, current, squashed_blocked_transactions)

    def detect_deadlock(self, transaction, visited, current, blocked_dict):

        if transaction in blocked_dict:

            visited[transaction] = len(current) + 1
            current.append(transaction)

            for block in blocked_dict[transaction]:

                block = block[0]

                if self.transaction_map[block].get_status() == \
                        TransactionStatus.ABORTED:
                    continue
                if block in visited:
                    self.clear_deadlock(current, visited[block] - 1)
                else:
                    self.detect_deadlock(block, visited, current,
                                         blocked_dict)

            current.pop()
            visited.pop(transaction)

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

    def get_squashed_blocked_transactions(self):

        squashed_blocked_transactions = defaultdict(list)

        for blocked_dicts in self.blocked_transactions.values():

            for transaction_name, blocked_tuple in blocked_dicts.items():

                squashed_blocked_transactions[
                    transaction_name].append(blocked_tuple)

        return squashed_blocked_transactions

    def blocked_to_waiting(self):

        to_pop = list()

        squashed_blocked_transactions = \
            self.get_squashed_blocked_transactions()

        for blocked_dict_key in sorted(self.blocked_transactions.keys()):
            items = self.blocked_transactions[blocked_dict_key].items()
            for key, blocked_tuple in items:

                is_clear = True

                # for blocking_transaction in block:

                block = self.transaction_map[blocked_tuple[0]]
                is_aborted = block.get_status() == TransactionStatus.ABORTED
                is_committed = block.get_status() == \
                    TransactionStatus.COMMITTED
                is_clear = is_clear & (is_aborted or is_committed)
                if is_clear:

                    to_delete = None

                    for blocker_tuple in squashed_blocked_transactions[key]:

                        if blocker_tuple[0] == block.name:
                            to_delete = blocker_tuple
                            break

                    squashed_blocked_transactions[key].remove(to_delete)
                    to_pop.append((blocked_dict_key, key))

                    if len(squashed_blocked_transactions[key]) == 0:

                        self.waiting_transactions[self.current_time][
                            key] = blocked_tuple[1:]
                        transaction = self.transaction_map[key]
                        transaction.set_status(TransactionStatus.WAITING)

        for key in to_pop:
            self.blocked_transactions[key[0]].pop(key[1])

    def abort(self, name):

        squashed_blocked_transactions = \
            self.get_squashed_blocked_transactions()

        to_pop_blocked = list()
        to_pop_waiting = list()

        if name in squashed_blocked_transactions:

            squashed_blocked_transactions.pop(name)
            # self.blocked_transactions.pop(name)

            for blocked_dict_key in sorted(self.blocked_transactions.keys()):

                if name in self.blocked_transactions[blocked_dict_key]:

                    to_pop_blocked.append((blocked_dict_key, name))
                    # self.blocked_transactions[blocked_dict_key].pop(name)

            for key in to_pop_blocked:
                self.blocked_transactions[key[0]].pop(key[1])
        # if name in self.waiting_transactions:
        #     self.waiting_transactions.pop(name)

        for time in self.waiting_transactions.keys():

            if name in self.waiting_transactions[time]:
                to_pop_waiting.append((time, name))

        for key in to_pop_waiting:
            self.waiting_transactions[key[0]].pop(key[1])

        transaction = self.transaction_map[name]
        transaction.set_status(TransactionStatus.ABORTED)
        self.clear_locks(transaction)

        return

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

        for time, waiting_dicts in self.waiting_transactions.items():

            for transaction in waiting_dicts.keys():

                params = waiting_dicts[transaction]
                transaction_obj = self.transaction_map[transaction]

                if params[0] == InstructionType.WRITE:
                    self.write_request((transaction, params[1], params[2]))

                elif params[0] == InstructionType.READ:
                    self.read_request((transaction, params[1]))

                elif params[0] == InstructionType.READ_ONLY:
                    self.read_request((transaction, params[1]), True)

                if self.transaction_map[transaction].get_status() == \
                   TransactionStatus.RUNNING:
                    to_pop.append((time, transaction))

        for key in to_pop:
            self.waiting_transactions[key[0]].pop(key[1])

    def commit_transaction(self, name):

        status = self.transaction_map[name].get_status()

        if status == TransactionStatus.COMMITTED or \
                status == TransactionStatus.ABORTED:
            return

        transaction = self.transaction_map[name]
        read_variables = transaction.get_read_variables()

        for variable, values in read_variables.items():

            for value in values:

                log.info(name + " read the value " +
                         str(value) + " of variable " + variable)

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
        if status == TransactionStatus.COMMITTED or \
                status == TransactionStatus.ABORTED:
            return

        self.commit_transaction(params[0])
        log.info(params[0] + " committed")
        self.clear_locks(self.transaction_map[params[0]])

        squashed_blocked_transactions = \
            self.get_squashed_blocked_transactions()

        to_pop_blocked = list()
        to_pop_waiting = list()

        if params[0] in squashed_blocked_transactions:

            squashed_blocked_transactions.pop(params[0])
            # self.blocked_transactions.pop(params[0])

            for blocked_dict_key in sorted(self.blocked_transactions.keys()):

                if params[0] in self.blocked_transactions[blocked_dict_key]:

                    to_pop_blocked.append((blocked_dict_key, params[0]))
                    # self.blocked_transactions[blocked_dict_key].pop(params[0])

            for key in to_pop_blocked:
                self.blocked_transactions[key[0]].pop(key[1])

        for time in self.waiting_transactions.keys():

            if params[0] in self.waiting_transactions[time]:
                to_pop_waiting.append((time, params[0]))

        for key in to_pop_waiting:
            self.waiting_transactions[key[0]].pop(key[1])

        self.detect_and_clear_deadlocks()
        self.blocked_to_waiting()
        self.try_waiting()
