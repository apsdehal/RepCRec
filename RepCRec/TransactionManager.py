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
            # print(params[0])
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

            self.lock_table.set_lock(transaction, LockType.WRITE, variable)
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

            squashed_waiting_transactions = self.get_squashed_waiting_transactions()

            if transaction.name in squashed_waiting_transactions:

                for waiting_tuple in squashed_waiting_transactions[transaction.name]:

                    if waiting_tuple[1] != variable:
                        return

            transaction.set_status(TransactionStatus.RUNNING)

        elif lock_acquire_status == LockAcquireStatus.ALL_SITES_DOWN:

            waiting_txn_tuple = (InstructionType.WRITE,
                                 variable,
                                 value)

            squashed_waiting_transactions = self.get_squashed_waiting_transactions()

            if transaction.name in squashed_waiting_transactions:

                if waiting_txn_tuple in squashed_waiting_transactions[transaction.name]:
                    return

            # for time in list(self.waiting_transactions):

            #     for t_name in self.waiting_transactions[time]:

            #         if t_name == transaction.name and \
            #                 self.waiting_transactions[time][t_name] == waiting_txn_tuple:
            #             return

            log.info(transaction.name + " is waiting on " + variable)

            transaction.set_status(TransactionStatus.WAITING)

            self.waiting_transactions[self.current_time][
                transaction.name] = waiting_txn_tuple

        else:

            for lock in self.site_manager.get_set_locks().lock_map[variable]:

                blocking_transaction = lock.transaction.name

                if lock.transaction == transaction:
                    continue

                blocking_txn_tuple = (blocking_transaction,
                                      InstructionType.WRITE,
                                      variable,
                                      value)

                squashed_blocking_transactions = self.get_squashed_blocked_transactions()

                if transaction.name in squashed_blocking_transactions:

                    if blocking_txn_tuple in squashed_blocking_transactions[transaction.name]:
                        return

                log.info(transaction.name +
                         " is blocked for a write lock by " +
                         blocking_transaction + " on " + variable)
                transaction.set_status(TransactionStatus.BLOCKED)

                self.current_time += 1

                self.blocked_transactions[self.current_time][
                    transaction_name] = blocking_txn_tuple

    def read_request_read_only(self, transaction, variable, transaction_name,
                               try_waiting):
        """
        Method responsible for processing a read request from a read only
        transaction, if the site holding the variable is down,
        it waits for the site to be up and then reads the variable.

        Args:
            params : list of parameters of the parsed instruction, containing
                     instruction name

        """
        if try_waiting:

            val = self.site_manager.get_current_variables(variable)

            if val is None:

                transaction.variable_values[variable] = val
            else:
                return

        if variable in transaction.variable_values:

            if variable not in transaction.read_variables:
                transaction.read_variables[variable] = list()

            transaction.read_variables[
                variable].append(transaction.variable_values[variable])

            squashed_waiting_transactions = self.get_squashed_waiting_transactions()

            if transaction.name in squashed_waiting_transactions:

                for waiting_tuple in squashed_waiting_transactions[transaction.name]:

                    if waiting_tuple[1] != variable:
                        return

            transaction.set_status(TransactionStatus.RUNNING)

        else:

            waiting_txn = (InstructionType.READ_ONLY, variable)

            squashed_waiting_transactions = self.get_squashed_waiting_transactions()

            if transaction_name in squashed_waiting_transactions:

                if waiting_txn_tuple in squashed_waiting_transactions[transaction_name]:
                    return

            transaction.set_status(TransactionStatus.WAITING)
            log.info(transaction.name + " is waiting on " + variable)

            self.waiting_transactions[self.current_time][
                transaction_name] = waiting_txn

        return

    def read_request(self, params, try_waiting=False):
        """
        Method responsible for processing a read request, gets read locks on
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

            if self.lock_table.is_locked_by_transaction(transaction, variable, LockType.WRITE):

                val = transaction.uncommitted_variables[variable]

                log.info(transaction.name + " got read lock on " +
                         variable + " having value " + str(val))

                if variable not in transaction.read_variables:
                    transaction.read_variables[variable] = list()

                transaction.read_variables[variable].append(val)

                return

            if self.lock_table.is_locked_by_transaction(transaction, variable, LockType.READ):

                self.lock_table.set_lock(transaction, LockType.READ, variable)
                log.info(transaction.name +
                         " already has a read lock on " + variable)
                transaction.set_status(TransactionStatus.RUNNING)
                return

            for time in list(self.blocked_transactions):

                blocked_tuple_dict = self.blocked_transactions[time]

                for key in list(blocked_tuple_dict):

                    blocked_tuple = blocked_tuple_dict[key]
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

                curr_variable = self.site_manager.get_current_variables(
                    variable)
                transaction.read_variables[
                    variable].append(curr_variable)

                self.lock_table.set_lock(transaction,
                                         LockType.READ, variable)

                squashed_waiting_transactions = self.get_squashed_waiting_transactions()

                if transaction.name in squashed_waiting_transactions:

                    for waiting_tuple in squashed_waiting_transactions[transaction.name]:

                        if waiting_tuple[1] != variable:
                            return

                transaction.set_status(TransactionStatus.RUNNING)

            elif lock_acquire_status == LockAcquireStatus.ALL_SITES_DOWN:

                waiting_txn_tuple = (InstructionType.READ, variable)

                squashed_waiting_transactions = self.get_squashed_waiting_transactions()

                if transaction.name in squashed_waiting_transactions:

                    if waiting_txn_tuple in squashed_waiting_transactions[transaction.name]:
                        return

                log.info(transaction.name + " is waiting on " + variable)

                transaction.set_status(TransactionStatus.WAITING)

                self.waiting_transactions[self.current_time][
                    transaction.name] = waiting_txn_tuple

            else:

                for lock in self.site_manager.get_set_locks().lock_map[variable]:

                    blocking_transaction = lock.transaction.name

                    if lock.transaction == transaction:
                        continue

                    blocking_txn_tuple = (blocking_transaction,
                                          InstructionType.READ,
                                          variable)

                    squashed_blocking_transactions = self.get_squashed_blocked_transactions()

                    if transaction.name in squashed_blocking_transactions:

                        if blocking_txn_tuple in squashed_blocking_transactions[transaction.name]:
                            return

                    log.info(transaction.name + " is blocked by " +
                             blocking_transaction + " on " + variable)
                    transaction.set_status(TransactionStatus.BLOCKED)

                    # if transaction_name not in self.blocked_transactions:
                    #     self.blocked_transactions[transaction_name] = []

                    self.blocked_transactions[self.current_time][
                        transaction_name] = blocking_txn_tuple

                    self.current_time += 1
        return

    def clear_aborted(self):
        """
        Method responsible for clearing transactions
        aborted due to site failure. Excplicitely calls
        the abort function for them.
        """

        to_pop = list()

        for trn_name in list(self.transaction_map):
            transaction = self.transaction_map[trn_name]
            if transaction.get_status() == TransactionStatus.ABORTED:

                to_pop.append(trn_name)
                self.abort(trn_name)

    def detect_and_clear_deadlocks(self):
        """
        Method responsible for detecting and clearing deadlocks.
        Traverses through blocked transactions and tries to detect deadlock
        on each of them.
        """

        squashed_blocked_transactions = \
            self.get_squashed_blocked_transactions()

        for x in list(squashed_blocked_transactions):

            visited = dict()
            current = []
            self.detect_deadlock(
                x, visited, current, squashed_blocked_transactions)

    def detect_deadlock(self, transaction, visited, current, blocked_dict):
        """
        Method responsible for detecting deadlock and calling the
        clear_deadlock function when necessary.

        Args:
            transaction (str) : name of the blocked transaction
            visited (dict) : stores index of each transaction visited
            current (list) : list of all transactions which may be
                             involved in a deadlock
            blocked_list (dict) : stores blocked transactions and
                                  the tuple containing information
                                  about the blocker
        """

        is_aborted = self.transaction_map[transaction].get_status() \
            == TransactionStatus.ABORTED
        is_committed = self.transaction_map[transaction].get_status() \
            == TransactionStatus.COMMITTED

        if transaction in blocked_dict and not is_aborted and not is_committed:

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
        """
        Method responsible for resolving deadlock after
        it has been detected by aborting
        the youngest transaction involved.

        Args:
            transaction_list (list) : list of all transactions visited

            index (int) : index of the first transaction involved in deadlock
        """

        transaction_list = transaction_list[index:]
        max_id = -1
        max_name = None

        for name in transaction_list:
            transaction = self.transaction_map[name]
            is_committed = transaction.get_status() == \
                TransactionStatus.ABORTED
            is_aborted = transaction.get_status() == \
                TransactionStatus.COMMITTED
            if is_committed or is_aborted:
                return

            if max_id < transaction.id:
                max_id = self.transaction_map[name].id
                max_name = name

        log.info(max_name + " aborted as it is youngest in a deadlock")
        self.abort(max_name)

    def get_squashed_blocked_transactions(self):
        """
        Method responsible for squashing the default dict
        self.blocked_transactions (based on current time as key)
        into a default dict based on transaction name as key.

        """

        squashed_blocked_transactions = defaultdict(list)

        for blocked_dicts in self.blocked_transactions.values():

            for transaction_name in list(blocked_dicts):
                blocked_tuple = blocked_dicts[transaction_name]
                squashed_blocked_transactions[
                    transaction_name].append(blocked_tuple)

        return squashed_blocked_transactions

    def get_squashed_waiting_transactions(self):
        """
        Method responsible for squashing the default dict
        self.waiting_transactions (based on current time as key)
        into a default dict based on transaction name as key.
        """

        squashed_waiting_transactions = defaultdict(list)

        for waiting_dicts in self.waiting_transactions.values():

            for transaction_name in list(waiting_dicts):

                waiting_tuple = waiting_dicts[transaction_name]

                squashed_waiting_transactions[
                    transaction_name].append(waiting_tuple)

        return squashed_waiting_transactions

    def blocked_to_waiting(self):
        """
        Method responsible for trying to resolve blocked transactions
        by checking if the transaction blocking them has been
        committed or aborted. If so, it changes the status of the blocked
        transaction to waiting.
        """

        to_pop = list()

        squashed_blocked_transactions = \
            self.get_squashed_blocked_transactions()

        for blocked_dict_key in sorted(self.blocked_transactions.keys()):

            items = list(self.blocked_transactions[blocked_dict_key])

            for key in items:
                blocked_tuple = self.blocked_transactions[
                    blocked_dict_key][key]
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

                        flag = 0
                        squashed_waiting_transactions = self.get_squashed_waiting_transactions()

                        if key in squashed_waiting_transactions:

                            for bl_tuple in squashed_waiting_transactions[key]:

                                if bl_tuple == blocked_tuple[1:]:
                                    flag = 1
                                    break
                        if flag:
                            continue

                        self.waiting_transactions[self.current_time][
                            key] = blocked_tuple[1:]
                        transaction = self.transaction_map[key]
                        transaction.set_status(TransactionStatus.WAITING)

        for key in to_pop:
            self.blocked_transactions[key[0]].pop(key[1])

    def abort(self, name):
        """
        Method responsible for aborting transactions. It clears
        the transaction from blocked and waiting dicts.

        Args:
            name (str): name of the transaction to be aborted
        """

        to_pop_blocked = list()
        to_pop_waiting = list()

        for time in sorted(self.blocked_transactions.keys()):

            if name in self.blocked_transactions[time]:

                to_pop_blocked.append((time, name))
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
        """
        Method responsible for clearing locks of transactions. It clears
        the transaction locks in the global lock table as well as the
        lock tables controlled by site manager.

        Args:
            transaction (Object of class Transaction): transaction whose locks have to be cleared
        """

        lock_map = self.site_manager.get_set_locks().get_lock_map()

        for var_name in sorted(list(lock_map)):
            locks = lock_map[var_name]
            for lock in locks:
                if lock.transaction == transaction:
                    self.site_manager.clear_locks(lock, var_name)
                    log.debug("Clearing site locks for " + transaction.name +
                              " variable: " + var_name)

                    if self.lock_table.clear_lock(lock, var_name):
                        log.info("Clearing locks for " + transaction.name +
                                 " variable: " + var_name)

    def try_waiting(self):
        """
        Method responsible for getting locks for waiting transaactions.
        Traverses through the waiting transactions and tries to get
        locks for them, to change their status to running.

        """

        to_pop = list()

        for time in list(self.waiting_transactions):

            waiting_dicts = self.waiting_transactions[time]

            for transaction in list(waiting_dicts):

                params = waiting_dicts[transaction]
                transaction_obj = self.transaction_map[transaction]
                transaction_obj.set_status(TransactionStatus.WAITING)

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
        """
        Method responsible for commiting transactions when we
        recieve an end instruction. It traverses through the list
        of uncommitted variables and writes the values on the
        respective sites.

        """

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
        """
        Method called when we recieve an end instruction.
        Calls commit_transaction and clears the transaction
        from blocked and waiting dicts. Also clears locks
        held by transaction.

        Args:
            params(list) : contains the transaction name to be committed
        """

        status = self.transaction_map[params[0]].get_status()

        if status == TransactionStatus.COMMITTED or \
                status == TransactionStatus.ABORTED:
            return

        self.commit_transaction(params[0])
        # print("Got here")
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
