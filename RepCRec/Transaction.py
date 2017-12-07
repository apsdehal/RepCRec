"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
from .enums.TransactionStatus import TransactionStatus


class Transaction:

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
        return self.id

    def get_status(self):
        return self.status

    def is_read_only(self):
        return self.is_read_only

    def get_sites_accessed(self):
        return self.sites_accessed

    def set_status(self, status):

        if status in TransactionStatus:
            self.status = status
        else:
            # TODO: Do something here
            return

    def get_read_variables(self):
        return self.read_variables

    def get_uncommitted_variables(self):
        return self.uncommitted_variables

    def clear_uncommitted_variables(self):
        self.uncommitted_variables = dict()

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False
