import copy

from .enums.LockType import LockType


class Variable:

    def __init__(self, index, name):
        self.index = index
        self.name = name
        self.current_site_id = None
        self.value = 0
        self.lock_type = None

    @classmethod
    def get_sites(id):

        if id % 2 == 0:
            return 'all'
        else:
            return (id % 10) + 1

    def get_current_site(self):
        return self.current_site_id

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value

    def is_locked(self):
        return self.lock_type

    def set_lock_type(self, lock_type):
        self.lock_type = lock_type

    def replicate(self):
        return copy.deepcopy(self)
