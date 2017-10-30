import logging

from tornado import web, gen, process, httpserver, netutil
from .config import config
from .SiteHandler import SiteHandler
from .enums.SiteStatus import SiteStatus

log = logging.getLogger(__name__)


class Site:
    BASE_PORT = config['BASE_PORT']

    def __init__(self, index):
        self.id = index
        self.variables = []
        self.status = SiteStatus.DOWN
        self.last_failure_time = None

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

    def listen(self):
        application = web.Application([
            (r"/", SiteHandler,
             dict(variables=self.variables,
                  id=self.id))
        ])
        http_server = httpserver.HTTPServer(application)
        http_server.add_sockets(netutil.bind_sockets(
                                self.BASE_PORT + self.id))
        log.debug("Site %d listening on %d" %
                  (self.id, self.BASE_PORT + self.id))

        self.set_status(SiteStatus.UP)
