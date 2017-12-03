from tornado.ioloop import IOLoop

from .Site import Site
from .Variable import Variable
from .LockType import LockType
from .constants import FAIL_FUNC, DUMP_FUNC, RECOVER_FUNC

log = logging.getLogger(__name__)


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

        sites = self.get_site_range(sites)

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

    def get_site_range(self, sites):
        if sites == 'all':
            sites = range(1, self.num_sites + 1)
        else:
            sites = [sites]

    def tick(self, instruction):
        if instruction.get_instruction_type() == DUMP_FUNC:
            log.debug("Got here")

            if len(params[0]) == 0:
                for site in self.site_manager.sites[1:]:
                    site.dump_site()

            elif params[0][0] == 'x':
                sites = Variable.get_sites(int(params[0][1:]))
                sites = self.get_site_range(sites)

                for site in sites:
                    variables = self.sites[site].get_all_variables()

                    for variable in variables:
                        if variable.name == params[0]:
                            log.info(variable.value)

            elif len(params[0]) == 2:
                site = self.site_manager.get_site(int(params[0]))
                site.dump_site()

        elif instruction.get_instruction_type() == FAIL_FUNC:
            self.site_manager.fail(int(params[0]))

        elif instruction.get_instruction_type() == RECOVER_FUNC:
            self.site_manager.recover(int(params[0]))

    def clear_locks(self, lock, variable_name):
        sites = Variable.get_sites(variable_name)
        sites = self.get_site_range(sites)

        for index in sites:
            site = self.sites[index]

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
