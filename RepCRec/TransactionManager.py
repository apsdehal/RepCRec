# Revise this module again according to design doc
class TransactionManager:
    def __init__(self, num_vars, num_sites):
        self.number_of_variables = num_vars
        self.number_of_sites = num_sites
        self.transaction_map = dict()
        self.current_time = 0

    def commit_transaction(self, name):
        return

    def tick(self, instruction):
        return

    def begin(self, name):
        return

    def begin_read_only(self, name):
        return

    def dump(self, id=None, name=None):
        return

    def write_request(self, id, variable, value):
        return

    def write_request_even(self, transaction, variable, value):
        return

    def end(self, num, name):
        return
