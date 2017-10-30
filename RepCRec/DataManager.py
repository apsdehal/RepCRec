from LockTable import LockTable


class DataManager:
    def __init__(self, id):
        self.site_id = id
        self.lock_table = LockTable()
        self.variable_map = dict()

    def add_variable(self, name, variable):
        # TODO: Add check for already existing variable
        self.variable_map[name] = variable

    def get_variable(self, name):
        if name in self.variable_map:
            return self.variable_map[name]
        else:
            return None

    def has_variable(self, name):
        if name in self.variable_map:
            return True
        else:
            return False

    def get_lock_table(self):
        return self.lock_table

    def fail(self):
        return

    def recover(self):
        return

    def get_lock(self, transaction, lock_type, variable_name):
        return

    def write_variable(self, transaction, variable_name, value):
        return

    def read_variable(self, transaction, variable_name):
        return None

    def get_variables(self):
        return self.variable_map
