"""
Authors:
Sharan Agrawal
Amanpreet Singh
"""
import logging

from tornado.ioloop import IOLoop

from .Site import Site
from .Variable import Variable
from .LockTable import LockTable
from .enums.LockType import LockType
from .enums.SiteStatus import SiteStatus
from .enums.LockAcquireStatus import LockAcquireStatus
from .constants import FAIL_FUNC, DUMP_FUNC, RECOVER_FUNC

log = logging.getLogger(__name__)


class SiteManager:
    """
    Site Manager is the component of whole system which manages sites
    and the interactions related to them, including fail, recover.
    It is the central point of contact for transaction manager for
    any of the queries related to site including access to lock table
    through data manager. Site Manager is responsible for providing
    all kind of access and act as proxy for different sites

    Args:
        num_site: Number of sites
        num_variables: Number of total variables present
    """

    def __init__(self, num_sites, num_variables):
        # Append None on zero index for easy retreival
        self.num_sites = num_sites
        self.sites = [None] + [Site(i) for i in range(1, num_sites + 1)]
        self.num_variables = num_variables

    def tick(self, instruction):
        """
        IO calls the tick of Site Manager in specific cases when the
        Instruction has something to do with sites. This includes
        instructions for fail, recover and dump.

        Args:
            instruction: Instruction object for next instruction
        """
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
        """
        Checks whether an index passed to site manager is valid
        Otherwise raise a value error
        Args:
            index: Index value to be test
        Raises:
            ValueError
        """
        if index > self.num_sites or index <= 0:
            raise ValueError("Index must be in range %d to %d" %
                             (1, self.num_sites))

    def get_site(self, index):
        """
        Returns a site on particular index
        Args:
            index: Index of the site to be returned
        Returns:
            Site object present at index passed
        """
        self._check_index_sanity(index)
        return self.sites[index]

    def get_locks(self, transaction, typeof, variable):
        """
        Tries to provide a lock for a variable to a transaction
        Various check ensure that if a legitimate lock can be
        provided to the transaction

        Args:
            transaction: Transaction which wants the lock
            typeof: Type of lock to be acquired WRITE or READ
            variable: variable name on which lock is requested.
        Returns:
            Boolean telling whether a lock was successfully
            acquired or not
        """
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
        """
        A helper function which returns range of all sites if 'all'
        is passed, otherwise returns a list containing only single
        index passed

        Args:
            sites: string or number 'all' or a number
        Returns:
            list containing site indices
        """
        if sites == 'all':
            sites = range(1, self.num_sites + 1)
        else:
            sites = [sites]
        return sites

    def get_current_variables(self, var=None):
        """
        Returns currently set variables of for a variable if passed, else
        all of them

        Args:
            var: If none, all variable values will be returned else
                 value will be returned for var
        Returns:
            dict containing values for variables
        """
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

                    if variable.name in site.recovered_variables:

                        if var is not None and variable.name == var:
                            return variable.value

                        variable_values[variable.name] = variable.value

            if len(variable_values) == self.num_variables:
                return variable_values

        if var is None:
            return variable_values
        else:
            return None

    def get_set_locks(self):
        """
        Utility function to get all of the locks set in any
        of the site's data manager

        Returns:
            A dict containing variable name as key and list
            of locks present anywhere.
        """
        locks = dict()

        for site in self.sites[1:]:
            lock_map = site.data_manager.lock_table.lock_map

            for var, curr_locks in lock_map.items():
                if var not in locks:
                    locks[var] = []

                for lock in curr_locks:
                    if lock not in locks[var]:
                        locks[var].append(lock)
        lock_table = LockTable()
        lock_table.lock_map = locks
        return lock_table

    def clear_locks(self, lock, variable_name):
        """
        Clears a particular lock for for a variable

        Args:
            lock: Lock to be cleared
            variable_name: Variable for which the lock is to
                           be cleared
        """

        sites = Variable.get_sites(variable_name)
        sites = self.get_site_range(sites)

        for index in sites:
            site = self.sites[index]
            site.clear_lock(lock, variable_name)

    def start(self):
        """
        Starts all of the sites
        """
        for site in self.sites[1:]:
            site.listen()

    def fail(self, index):
        """
        Fail a particular site

        Args:
         index: Index of the site to be failed
        """
        self._check_index_sanity(index)
        log.info("Site " + str(index) + " failed")
        self.sites[index].fail()

    def recover(self, index):
        """
        Recover a particular site

        Args:
         index: Index of the site to be recovered
        """

        self._check_index_sanity(index)
        log.info("Site " + str(index) + " recovered")
        self.sites[index].recover()
