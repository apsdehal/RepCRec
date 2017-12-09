"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from .enums.LockType import LockType
from .Lock import Lock


class LockTable:
    """
    LockTable is an entity which contains information about
    locks present on a variable. Main attribute is lock_map
    which contains variables a key and list of locks as values
    It helps in setting, testing and clearing locks
    """

    def __init__(self):
        # Variable Index to lock map
        self.lock_map = dict()

    def get_lock_map(self):
        """
        Returns the lock map of the locktable
        """
        return self.lock_map

    def get_len_locks(self, variable):
        """
        Returns number of locks for a variable

        Returns:
            Number of locks for a variable
        """
        if variable in self.lock_map:
            return len(self.lock_map[variable])
        else:
            return 0

    def set_lock(self, transaction, lock_type, variable):
        """
        Tries to set a particular type of lock on variable
        for a transaction

        Args:
            transaction: Transaction which wants the lock
            lock_type: Lock Type to be set
            variable: Variable on which the lock is required
        """
        lock = Lock(lock_type, transaction)

        if variable not in self.lock_map:
            self.lock_map[variable] = []

        for org in self.lock_map[variable]:
            if org == lock:
                return
        self.lock_map[variable].append(lock)

    def is_locked(self, variable):
        """
        Checks whether a lock is set on variable

        Args:
            variable: Variable for which lock is to be checked
        Returns:
            bool telling whether the lock is set
        """
        if variable not in self.lock_map:
            return False
        else:
            if len(self.lock_map[variable]) == 0:
                return False
            return True

    def is_write_locked(self, variable):
        """
        Checks whether a write lock is set on variable

        Args:
            variable: Variable for which write lock is to be checked
        Returns:
            bool telling whether the write lock is set
        """
        if variable not in self.lock_map:
            return False
        else:
            for lock in self.lock_map[variable]:
                if lock.get_lock_type() == LockType.WRITE:
                    return True
            else:
                return False

    def is_read_locked(self, variable):
        """
        Checks whether a read lock is set on variable

        Args:
            variable: Variable for which read lock is to be checked
        Returns:
            bool telling whether the read lock is set
        """
        if variable not in self.lock_map:
            return False
        else:
            for lock in self.lock_map[variable]:
                if lock.get_lock_type() == LockType.WRITE:
                    return True
            else:
                return False

    def free(self, variable):
        """
        Frees all locks of a variable

        Args:
            variable: Variable for which locks are to be freed
        """
        self.lock_map.pop(variable)

    def clear_lock(self, lock, variable):
        """
        Clear a particular lock from the variable

        Args:
            lock: Lock to be cleared
            variable: variable on which lock is to cleared
        """
        if variable in self.lock_map.keys():
            try:
                index = self.lock_map[variable].index(lock)
                self.lock_map[variable] = self.lock_map[variable][
                    :index] + self.lock_map[variable][index + 1:]
                if len(self.lock_map[variable]) == 0:
                    self.lock_map.pop(variable)
                return True
            except ValueError:
                pass
        return False

    def is_locked_by_transaction(self, current_transaction, variable,
                                 lock_type=None):
        """
        Tells whether a variable is locked by the transaction

        Args:
            current_transaction: Transaction we are talking about
            variable: variable to be tested
            lock_type: If we have to check for a particular lock type
        Returns:
            0 if not present or if present and lock type doesn't match
            1 if present and lock_type is passed and matches
            1 if present and no lock type passed
        """

        if variable in self.lock_map:
            for lock in self.lock_map[variable]:
                transaction = lock.get_transaction()
                if current_transaction.get_id() == transaction.get_id():
                    if lock_type is None:
                        return 1
                    elif lock_type == lock.get_lock_type():
                        return 1
            return 0
        else:
            return 0
