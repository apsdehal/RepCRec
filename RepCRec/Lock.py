"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from .enums.LockType import LockType
from .Transaction import Transaction


class Lock:
    """
    Represents a lock on variable by a transaction

    Args:
        lock_type: Lock type of the lock
        transaction: Transaction which helds the lock
    """

    def __init__(self, lock_type=None, transaction=None):
        # TODO Think if we need an id for a lock
        self.lock_type = lock_type
        self.transaction = transaction

    def get_transaction(self):
        """
        Get the transaciton of the lock

        Return:
            transaction of the lock
        """
        return self.transaction

    def get_lock_type(self):
        """
        Get the lock_type of the lock

        Return:
            lock_type of the lock
        """
        return self.lock_type

    def set_lock_type(self, lock_type):
        """
        Sets the lock type of the lock

        Args:
            lock_type: Lock type to be set
        Raises:
            ValueError is lock passed is not of LockType class
        """
        if lock_type in LockType:
            self.lock_type = lock_type
        else:
            raise ValueError("Not a valid lock type")

    def set_transaction(self, transaction=None):
        """
        Sets the transaction of the lock

        Args:
            transaction: transaction to be set
        Raises:
            RuntimeError: If transaction is not a valid transaction
        """
        if isinstance(transaction, Transaction) \
           or transaction is None:
            self.transaction = transaction
        else:
            raise RuntimeError("Not a valid transaction")

    def __eq__(self, other):
        """
        Used for comparing this lock to others
        """
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False
