from .enums.LockType import LockType
from .Lock import Lock


class LockTable:

    def __init__(self):
        # Variable Index to lock map
        self.lock_map = dict()
        self.lock_queue = dict()

    def get_lock_map(self):
        return self.lock_map

    def set_lock(self, transaction, lock_type, variable):
        lock = Lock(lock_type, transaction)
        self.lock_map[variable] = lock

    def is_locked(self, variable):
        if variable not in self.lock_map:
            return False
        else:
            return True

    def is_write_locked(self, variable):
        if variable not in self.lock_map:
            return False
        else:
            if self.lock_map[variable].get_lock_type() == LockType.WRITE:
                return True
            else:
                return False

    def is_read_locked(self, variable):
        if variable not in self.lock_map:
            return False
        else:
            if self.lock_map[variable].get_lock_type() == LockType.READ:
                return True
            else:
                return False

    def free(self, variable):
        self.lock_map.pop(variable)

    def clear_lock(self, lock, variable):
        if variable in self.lock_map:
            if self.lock_map[variable] == lock:
                self.free(variable)

    """
    Return 0 if not present or if present and lock type doesn't match
    Return 1 if present and lock_type matches
    """
    def is_locked_by_transaction(self, current_transaction, variable, lock_type):
        if variable in self.lock_map:
            lock = self.lock_map[variable]
            transaction = lock.get_transaction()
            if current_transaction.get_id() == transaction.get_id():
                if lock_type == lock.get_lock_type():
                    return 1
                else:
                    return 0
            else:
                return 0
        else:
            return 0
