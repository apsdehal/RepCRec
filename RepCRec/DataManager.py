import logging

from .LockTable import LockTable
from .Variable import Variable
from .enums.LockType import LockType

log = logging.getLogger(__name__)


class DataManager:

    def __init__(self, id):
        self.site_id = id
        self.lock_table = LockTable()
        self.variable_map = dict()

        for i in range(1, 21):

            if i % 2 == 0 or (1 + i % 10) == id:
                variable = Variable(i, 'x' + str(i), 10 * i, self.site_id)
                self.variable_map['x' + str(i)] = variable

    def add_variable(self, name, variable):
        # TODO: Add check for already existing variable
        self.variable_map[name] = variable

    def get_variable(self, name):
        if name in self.variable_map:
            return self.variable_map[name]
        else:
            return None

    def has_variable(self, name):
        if name in self.variable_map:
            return True
        else:
            return False

    def clear_lock(self, lock, variable):
        self.lock_table.clear_lock(lock, variable)

    def get_lock_table(self):
        return self.lock_table

    def fail(self):
        return

    def recover(self):
        return

    def get_lock(self, transaction, lock_type, variable):
        is_locked_by_txn = self.lock_table.is_locked_by_transaction(transaction,
                                                                    variable)

        if is_locked_by_txn:
            if self.lock_table.get_len_locks(variable) == 1:
                return True
            else:
                return False

        if lock_type == LockType.WRITE and \
           not self.lock_table.is_locked(variable):
            self.lock_table.set_lock(transaction, lock_type, variable)
            return True
        elif lock_type == LockType.READ and not self.lock_table.is_write_locked(variable):
            self.lock_table.set_lock(transaction, lock_type, variable)
            return True
        else:
            log.debug(transaction.name + " did not get write lock on " +
                      variable + " site: " + str(self.site_id))
            return False

    def write_variable(self, transaction, variable_name, value):
        # if self.lock_table.is_locked_by_transaction(transaction,
        #                                             variable_name,
        #                                             LockType.WRITE):
        self.variable_map[variable_name].set_value(value)
            # return True
        # else:
        #     return False

    def read_variable(self, transaction, variable_name):
        return None

    def get_variables(self):
        return self.variable_map
