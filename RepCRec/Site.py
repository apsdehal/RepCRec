"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
import logging

from tornado import web, gen, process, httpserver, netutil
from .config import config
from .SiteHandler import SiteHandler
from .DataManager import DataManager
from .Transaction import Transaction

from .enums.SiteStatus import SiteStatus
from .enums.TransactionStatus import TransactionStatus

log = logging.getLogger(__name__)


class Site:
    """
    Site is a component which demonstrates a particular site
    as mentioned in project requirement
    It listens on a port and has a data manager which contains
    lock table and variable themselves.
    Anyone wanting access to any variable that is present on this
    site will have to go through SiteManager and then Site.

    Args:
        index: Index of the current site
    """
    BASE_PORT = config['BASE_PORT']

    def __init__(self, index):
        self.id = index

        # Variables are mainly in DataManager, here only for convenience
        self.variables = []
        self.status = SiteStatus.UP
        self.last_failure_time = None
        self.data_manager = DataManager(self.id)
        self.recovered_variables = set()

        for i in range(1, 21):

            if i % 2 == 0 or (1 + i % 10) == self.id:
                self.recovered_variables.add('x' + str(i))

    def set_status(self, status):
        """
        Changes the status of the site
        Args:
            staus: TransactionStatus, the current status of site
        """

        if status in SiteStatus:
            self.status = status
        else:
            log.error("Invalid Site status")
        return

    def get_status(self):
        """
        Returns status of the site

        Returns:
            Status of the site
        """
        return self.status

    def get_id(self):
        """
        Returns index of the site

        Returns:
            index of the site
        """
        return self.id

    def get_last_failure_time(self):
        """
        Returns last failure time of the site

        Returns:
            last failure time of the site
        """
        return self.last_failure_time

    def set_last_failure_time(self, time):
        """
        Sets last failure time of the site

        Args:
            time: Set the last failure time of site
        """
        self.last_failure_time = time

    def get_lock(self, transaction, typeof, variable):
        """
        Tries to provide a transaction a lock on a variable.
        Has various checks to provide a legitimate lock.

        Args:
            transaction: Transaction which wants the lock
            typeof: type of lock
            variable: Variable on which lock is required
        """

        if self.data_manager.get_lock(transaction, typeof, variable):

            self.recovered_variables.add(variable)

            if len(self.recovered_variables) ==  \
                    len(self.data_manager.variable_map):
                self.status = SiteStatus.UP

            return True

        return False

    def clear_lock(self, lock, variable):
        """
        Clear a lock on a variable

        Args:
            lock: Lock to be removed
            variable: Variable on which lock is to removed
        """
        self.data_manager.clear_lock(lock, variable)

    def write_variable(self, transaction, variable, value):
        """
        Help a transaction write a value on the variable

        Args:
            transaction: Transaction which wants to write a value
            variable: Variable on which value is to be written
            value: Value to be written
        """
        if self.status != SiteStatus.DOWN and \
                variable in self.recovered_variables:

            self.data_manager.write_variable(transaction,
                                             variable,
                                             value)

    def listen(self):
        """
        Starts a website to listen on a port
        """
        # TODO: Actually kill the server instead of sending 500
        # See https://gist.github.com/mywaiting/4643396 mainly server.stop
        # and ioloop.kill for this instance

        application = web.Application([
            (r"/", SiteHandler,
             dict(variables=self.data_manager.variable_map,
                  index=self.id,
                  status=self.get_status()))
        ])
        http_server = httpserver.HTTPServer(application)
        http_server.add_sockets(netutil.bind_sockets(
            self.BASE_PORT + 20 * self.id))
        log.debug("Site %d listening on %d" %
                  (self.id, self.BASE_PORT + 20 * self.id))

        self.set_status(SiteStatus.UP)

    def fail(self):
        """
        Fails a website
        """
        self.set_status(SiteStatus.DOWN)
        self.recovered_variables = set()
        lock_table = self.data_manager.get_lock_table()

        lock_map = lock_table.get_lock_map()

        for variable, locks in lock_map.items():

            for lock in locks:
                log.info(lock.transaction.name + " aborted as site " +
                         str(self.id) + " failed")
                lock.transaction.set_status(TransactionStatus.ABORTED)

        # self.data_manager.lock_table.lock_map = dict()

    def recover(self):
        """
        Recover the site
        """
        # This would make sense once we actually kill the server

        for variable in self.data_manager.variable_map.keys():

            if int(variable[1:]) % 2 != 0:
                self.recovered_variables.add(variable)

        self.set_status(SiteStatus.RECOVERING)

    def dump_site(self):
        """
        Dumps the site
        """
        log.info("=== Site " + str(self.id) + " ===")

        if self.status == SiteStatus.DOWN:
            log.info("This site is down")
            return

        count = 0
        for index in list(self.data_manager.variable_map):

            variable = self.data_manager.variable_map[index]

            if self.status == SiteStatus.RECOVERING:

                count += 1

                if variable.name not in self.recovered_variables:
                    log.info(variable.name + ":" +
                             " is not available for reading")
                else:
                    log.info(variable.name + ": " + str(variable.value) +
                             " (available at site " + str(self.id) +
                             " for reading as it is the only" +
                             " copy or has been written after recovery)")
                continue

            if variable.value != int(index[1:]) * 10:
                count += 1
                log.info(variable.name + ":  " +
                         str(variable.value) + " at site " + str(self.id))

        if count != len(self.data_manager.variable_map):
            log.info("All other variables have same initial value")

    def get_all_variables(self):
        """
        Gets a list of variables present in data manager of this site

        Returns:
            A list of variables present on this site
        """
        variables = list()

        for idx in list(self.data_manager.variable_map):

            variable = self.data_manager.variable_map[idx]
            variables.append(variable)

        return variables
