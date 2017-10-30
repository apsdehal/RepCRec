from .enums.LockType import LockType
from .Transaction import Transaction


class Lock:
    def __init__(self, lock_type=None, transaction=None):
        # TODO Think if we need an id for a lock
        self.lock_type = lock_type
        self.transaction = transaction

    def get_transaction(self):
        return self.transaction

    def get_lock_type(self):
        return self.lock_type

    def set_lock_type(self, lock_type):
        if lock_type in LockType:
            self.lock_type = lock_type
        else:
            # TODO: Throw some kind of error here
            return

    def set_transaction(self, transaction=None):
        if isinstance(transaction, Transaction) \
           or transaction is None:
            self.transaction = transaction
        else:
            # TODO throw some kind of error here also
            return
