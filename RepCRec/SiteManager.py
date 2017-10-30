from tornado.ioloop import IOLoop

from .Site import Site


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
