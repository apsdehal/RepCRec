"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from .enums.LockType import LockType
from .Lock import Lock


class LockTable:

    def __init__(self):
        # Variable Index to lock map
        self.lock_map = dict()

    def get_lock_map(self):
        return self.lock_map

    def get_len_locks(self, variable):
        if variable in self.lock_map:
            return len(self.lock_map[variable])
        else:
            return 0

    def set_lock(self, transaction, lock_type, variable):
        lock = Lock(lock_type, transaction)

        if variable not in self.lock_map:
            self.lock_map[variable] = []

        for org in self.lock_map[variable]:
            if org == lock:
                return
        self.lock_map[variable].append(lock)

    def is_locked(self, variable):
        if variable not in self.lock_map:
            return False
        else:
            if len(self.lock_map[variable]) == 0:
                return False
            return True

    def is_write_locked(self, variable):
        if variable not in self.lock_map:
            return False
        else:
            for lock in self.lock_map[variable]:
                if lock.get_lock_type() == LockType.WRITE:
                    return True
            else:
                return False

    def is_read_locked(self, variable):
        if variable not in self.lock_map:
            return False
        else:
            for lock in self.lock_map[variable]:
                if lock.get_lock_type() == LockType.WRITE:
                    return True
            else:
                return False

    def free(self, variable):
        self.lock_map.pop(variable)

    def clear_lock(self, lock, variable):
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

    """
    Return 0 if not present or if present and lock type doesn't match
    Return 1 if present and lock_type is passed and matches
    Return 1 if present and no lock type passed
    """

    def is_locked_by_transaction(self, current_transaction, variable,
                                 lock_type=None):

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
