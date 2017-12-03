from tornado.ioloop import IOLoop

from .Site import Site
from .Variable import Variable
from .LockType import LockType


class SiteManager:

    def __init__(self, num_sites):
        # Append None on zero index for easy retreival
        self.num_sites = num_sites
        self.sites = [None] + [Site(i) for i in range(1, num_sites + 1)]

    def _check_index_sanity(self, index):
        if index > self.num_sites or index <= 0:
            raise ValueError("Index must be in range %d to %d" %
                             (1, self.num_sites))

    def get_site(self, index):
        self._check_index_sanity(index)
        return self.sites[index]

    def get_locks(self, transaction, typeof, variable):
        if type(variable) != int:
            variable = int(variable[1:])
        sites = Variable.get_sites(variable)

        if sites == 'all':
            sites = range(1, self.num_sites + 1)
        else:
            sites = [sites]

        flag = 0
        for site in sites:
            status = self.sites[site].get_status()
            if status == SiteStatus.DOWN:
                continue

            if status == SiteStatus.RECOVERING and typeof == LockType.READ:
                continue

            state = self.sites[site].get_lock(transaction, typeof, variable)
            if state == 1 and typeof == LockType.READ:
                return True
            flag &= state

        return flag

    def start(self):
        for site in self.sites[1:]:
            site.listen()
        IOLoop.current().start()

    def fail(self, index):
        self._check_index_sanity(index)
        self.sites[index].fail()

    def recover(self, index):
        self._check_index_sanity(index)
        self.sites[index].recover()
