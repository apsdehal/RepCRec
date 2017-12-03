import logging

from tornado import web, gen, process, httpserver, netutil
from .config import config
from .SiteHandler import SiteHandler
from .enums.SiteStatus import SiteStatus
from .DataManager import DataManager
from .Transaction import Transaction

log = logging.getLogger(__name__)


class Site:
    BASE_PORT = config['BASE_PORT']

    def __init__(self, index):
        self.id = index

        # Variables will be shifted to DataManager
        self.variables = []
        self.status = SiteStatus.UP
        self.last_failure_time = None
        self.data_manager = DataManager(self.id)

    def set_status(self, status):
        if status in SiteStatus:
            self.status = status
        else:
            # TODO: Throw an error here maybe?
            return

    def get_status(self):
        return self.status

    def get_id(self):
        return self.id

    def get_last_failure_time(self):
        return self.last_failure_time

    def set_last_failure_time(self, time):
        self.last_failure_time = time

    def get_lock(self, transaction, typeof, variable):
        return self.data_manager.get_lock(transaction, typeof, variable)

    def clear_lock(self, lock, variable):
        self.data_manager.clear_lock(lock, variable)

    def listen(self):
        # TODO: Actually kill the server instead of sending 500
        # See https://gist.github.com/mywaiting/4643396 mainly server.stop
        # and ioloop.kill for this instance
        application = web.Application([
            (r"/", SiteHandler,
             dict(variables=self.variables,
                  id=self.id,
                  status=self.get_status()))
        ])
        http_server = httpserver.HTTPServer(application)
        http_server.add_sockets(netutil.bind_sockets(
                                self.BASE_PORT + self.id))
        log.debug("Site %d listening on %d" %
                  (self.id, self.BASE_PORT + self.id))

        self.set_status(SiteStatus.UP)

    def fail(self):
        self.set_status(SiteStatus.DOWN)

    def recover(self):
        # This would make sense once we actually kill the server
        self.set_status(SiteStatus.RECOVERING)
        self.set_status(SiteStatus.UP)

    def dump_site(self):
        # TODO: Complete this once we have variables

        for index, variable in self.data_manager.variable_map.items():

            if isinstance(variable.value, int):
                print(variable.name + ":  " +
                      str(variable.value) + " at site " + str(self.id))

        return

    def get_all_variables(self):
        # TODO: Complete this once we have variables
        return
