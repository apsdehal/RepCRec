"""
Authors:
Sharan Agrawal
Amanpreet Singh
"""
import logging

from tornado.ioloop import IOLoop

from .Site import Site
from .Variable import Variable
from .enums.LockType import LockType
from .enums.SiteStatus import SiteStatus
from .enums.LockAcquireStatus import LockAcquireStatus
from .constants import FAIL_FUNC, DUMP_FUNC, RECOVER_FUNC

log = logging.getLogger(__name__)


class SiteManager:

    def __init__(self, num_sites, num_variables):
        # Append None on zero index for easy retreival
        self.num_sites = num_sites
        self.sites = [None] + [Site(i) for i in range(1, num_sites + 1)]
        self.num_variables = num_variables

    def tick(self, instruction):

        params = list(instruction.get_params())

        if instruction.get_instruction_type() == DUMP_FUNC:
            if len(params[0]) == 0:
                for site in self.sites[1:]:
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
                site = self.get_site(int(params[0]))
                site.dump_site()

        elif instruction.get_instruction_type() == FAIL_FUNC:
            self.fail(int(params[0]))

        elif instruction.get_instruction_type() == RECOVER_FUNC:
            self.recover(int(params[0]))

        return

    def _check_index_sanity(self, index):
        if index > self.num_sites or index <= 0:
            raise ValueError("Index must be in range %d to %d" %
                             (1, self.num_sites))

    def get_site(self, index):
        self._check_index_sanity(index)
        return self.sites[index]

    def get_locks(self, transaction, typeof, variable):
        sites = Variable.get_sites(variable)
        sites = self.get_site_range(sites)

        flag = 1
        recovering_flag = 0
        all_sites_down = 1
        even_index = int(variable[1:]) % 2 == 0

        for site in sites:

            status = self.sites[site].get_status()
            if status == SiteStatus.DOWN:
                continue

            if status == SiteStatus.RECOVERING and typeof == LockType.READ:

                if variable not in self.sites[site].recovered_variables:
                    continue

                elif not even_index:
                    recovering_flag = 1

            all_sites_down = 0

            state = self.sites[site].get_lock(transaction, typeof, variable)

            if state == 1 and typeof == LockType.READ:

                if recovering_flag:
                    return LockAcquireStatus.GOT_LOCK_RECOVERING
                else:
                    return LockAcquireStatus.GOT_LOCK
            flag &= state

        if all_sites_down == 1:
            return LockAcquireStatus.ALL_SITES_DOWN
        elif flag == 0:
            return LockAcquireStatus.NO_LOCK
        else:
            return LockAcquireStatus.GOT_LOCK

    def get_site_range(self, sites):
        if sites == 'all':
            sites = range(1, self.num_sites + 1)
        else:
            sites = [sites]
        return sites

    def get_current_variables(self, var=None):

        variable_values = dict()

        for site in self.sites[1:]:

            if site.status == SiteStatus.UP:
                variables = site.get_all_variables()

                for variable in variables:

                    if var is not None and variable.name == var:
                        return variable.value

                    variable_values[variable.name] = variable.value

                if len(variable_values) == self.num_variables:
                    return variable_values

            elif site.status == SiteStatus.RECOVERING:

                variables = site.get_all_variables()

                for variable in variables:

                    if variable.index % 2 == 1:

                        if var is not None and variable.name == var:
                            return variable.value

                        variable_values[variable.name] = variable.value

            if len(variable_values) == self.num_variables:
                return variable_values

        if var is None:
            return variable_values
        else:
            return None

    def clear_locks(self, lock, variable_name):

        sites = Variable.get_sites(variable_name)
        sites = self.get_site_range(sites)

        for index in sites:
            site = self.sites[index]
            site.clear_lock(lock, variable_name)

    def start(self):
        for site in self.sites[1:]:
            site.listen()

    def fail(self, index):
        self._check_index_sanity(index)
        log.info("Site " + str(index) + " failed")
        self.sites[index].fail()

    def recover(self, index):
        self._check_index_sanity(index)
        log.info("Site " + str(index) + " recovered")
        self.sites[index].recover()
