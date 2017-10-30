import logging

from tornado import web, gen, process, httpserver, netutil
from .config import config
from .SiteHandler import SiteHandler

log = logging.getLogger(__name__)


class Site:
    BASE_PORT = config['BASE_PORT']

    def __init__(self, index):
        self.index = index
        self.variables = []

    def listen(self):
        application = web.Application([
            (r"/", SiteHandler, dict(variables=self.variables))
        ])
        http_server = httpserver.HTTPServer(application)
        http_server.add_sockets(netutil.bind_sockets(
                                self.BASE_PORT + self.index))
        log.debug("Site %d listening on %d" %
                  (self.index, self.BASE_PORT + self.index))
