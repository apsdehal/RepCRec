"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
import logging

from .LockTable import LockTable
from .Variable import Variable
from .enums.LockType import LockType

log = logging.getLogger(__name__)


class DataManager:
    """
    DataManager is local to every site and manages the sites
    variables and locks

    Args:
        id: Id of the site on which current data manager is
    """

    def __init__(self, id):
        self.site_id = id
        self.lock_table = LockTable()
        self.variable_map = dict()

        for i in range(1, 21):

            if i % 2 == 0 or (1 + i % 10) == id:
                variable = Variable(i, 'x' + str(i), 10 * i, self.site_id)
                self.variable_map['x' + str(i)] = variable

    def add_variable(self, name, variable):
        """
        Adds variables to variable map

        Args:
            name: Name of the variable
            variable: Variable instance of the variable
        """
        self.variable_map[name] = variable

    def get_variable(self, name):
        """
        Returns variable instance given name of the variable

        Args:
            name: Name of the variable
        """
        if name in self.variable_map:
            return self.variable_map[name]
        else:
            return None

    def has_variable(self, name):
        """
        Tells whether variable is present on this site

        Args:
            name: Name of the variable to be checked
        Returns:
            boolean whether present or not
        """
        if name in self.variable_map:
            return True
        else:
            return False

    def clear_lock(self, lock, variable):
        """
        Clear lock for the variable

        Args:
            lock: Lock to be cleared
            variable: Variable for which the lock is to be cleared
        """
        self.lock_table.clear_lock(lock, variable)

    def get_lock_table(self):
        """
        Get the lock table of this data manager

        Returns:
            Lock table of this data manager
        """
        return self.lock_table

    def get_lock(self, transaction, lock_type, variable):
        """
        Tries to get a lock on variable for a transaction

        Args:
            transaction: Transaction which wants the lock
            lock_type: Type of the lock required
            variable: Variable on which lock is required
        Returns:
            Boolean according to whether lock was acquired or not
        """
        is_locked_by_txn = self.lock_table.is_locked_by_transaction(
            transaction,
            variable)
        if is_locked_by_txn:
            if self.lock_table.get_len_locks(variable) == 1:
                self.lock_table.set_lock(transaction,
                                         lock_type, variable)
                return True
            else:
                return False

        if lock_type == LockType.WRITE and \
           not self.lock_table.is_locked(variable):

            self.lock_table.set_lock(transaction, lock_type, variable)
            return True
        elif lock_type == LockType.READ and \
                not self.lock_table.is_write_locked(variable):
            self.lock_table.set_lock(transaction, lock_type, variable)
            return True

        else:

            if lock_type == LockType.WRITE:
                log.debug(transaction.name + " did not get write lock on " +
                          variable + " site: " + str(self.site_id))
            else:
                log.debug(transaction.name + " did not get read lock on " +
                          variable + " site: " + str(self.site_id))
            return False

    def write_variable(self, transaction, variable_name, value):
        """
        Write a value for a variable for a transaction

        Args:
            transaction: Transaction which wants to write the value
            variable_name: Variable whose value is to be written
            value: Value to be written

        Returns:
            Boolean whether write was successful or not
        """
        if self.lock_table.is_locked_by_transaction(transaction,
                                                    variable_name,
                                                    LockType.WRITE):
            self.variable_map[variable_name].set_value(value)
            return True
        else:
            return False

    def get_variables(self):
        """
        Getter for variable_map

        Returns:
            variable map of the data manager
        """
        return self.variable_map
