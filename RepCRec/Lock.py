"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
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
            raise ValueError("Not a valid lock type")

    def set_transaction(self, transaction=None):
        if isinstance(transaction, Transaction) \
           or transaction is None:
            self.transaction = transaction
        else:
            raise RuntimeError("Not a valid transaction")

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False
