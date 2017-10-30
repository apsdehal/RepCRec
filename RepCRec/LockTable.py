from enums.LockType import LockType


class LockTable:
    def __init__(self):
        # Variable Index to lock map
        self.lock_map = dict()

    def is_locked(self, variable_index):
        if variable_index not in self.lock_map:
            return False
        else:
            return True

    def is_write_locked(self, variable_index):
        if variable_index not in self.lock_map:
            return False
        else:
            if self.lock_map[variable_index].get_lock_type() == LockType.WRITE:
                return True
            else:
                return False

    def is_read_locked(self, variable_index):
        if variable_index not in self.lock_map:
            return False
        else:
            if self.lock_map[variable_index].get_lock_type() == LockType.READ:
                return True
            else:
                return False

    def free(self, variable_index):
        self.lock_map.pop(variable_index)

    """
    Return 0 if not present
    Return 1 if present and lock_type matches
    Return 2 if present but lock_type doesn't match
    """
    def is_locked_by_transaction(transaction, variable_index, lock_type):
        if variable_index in self.lock_map:
            lock = self.lock_map[variable_index]
            transaction = lock.get_transaction()
            if transaction.get_id() == transaction.get_id():
                if lock_type == lock.get_lock_type():
                    return 1
                else:
                    return 2
            else:
                return 0
        else:
            return 0
