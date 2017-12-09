"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from .enums.TransactionStatus import TransactionStatus


class Transaction:
    """
    Transaction is an entity that represents a running process
    as it was started via input file. This transaction can
    read and write over variables. Transaction can also be
    read only and has a status set to one of the TransactionStatus
    types

    Args:
        id: Id of the transaction (1, 2, 3 etc)
        name: Name of the transaction (T1, T2, etc)
        read_only: Boolean telling whether the transaction is
                   read only or not
    """

    def __init__(self, id, name, read_only=False):
        self.status = TransactionStatus.RUNNING
        self.id = id
        self.sites_accesssed = []
        self.name = name
        self.uncommitted_variables = dict()
        self.read_variables = dict()
        self.is_read_only = read_only
        self.variable_values = dict()

    def get_id(self):
        """
        Get id of the transaction

        Returns:
            Id of the transaction
        """
        return self.id

    def get_status(self):
        """
        Get staus of the transaction

        Returns:
            Status of the transaction
        """
        return self.status

    def is_read_only(self):
        """
        Tells whether transaction is read only

        Returns:
            Boolean telling whether read read only
        """
        return self.is_read_only

    def get_sites_accessed(self):
        """
        Gets sites accessed by the transaction

        Returns:
            List of sites accessed
        """
        return self.sites_accessed

    def set_status(self, status):
        """
        Set status of the transaction

        Args:
            status: TransactionStatus type to be set to this transaction's
                    status
        Raises:
            ValueError if unknown transactionstatus type is passed
        """
        if status in TransactionStatus:
            self.status = status
        else:
            raise ValueError("TransactionStatus is not valid")
            return

    def get_read_variables(self):
        """
        Get all the read variables of this transaction

        Returns:
            Dict of variables read
        """
        return self.read_variables

    def get_uncommitted_variables(self):
        """
        Get all the variables wrote by this transaction

        Returns:
            Dict of variables wrote
        """
        return self.uncommitted_variables

    def clear_uncommitted_variables(self):
        """
        Clear the uncommmitted variable of the transaction
        """
        self.uncommitted_variables = dict()

    def __eq__(self, other):
        """
        Compare this transaction with some other

        Returns:
            Boolean: Whether equal or not
        """
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False
