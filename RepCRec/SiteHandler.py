from tornado import gen, web

from .enums.SiteStatus import SiteStatus


class SiteHandler(web.RequestHandler):
    def initialize(self, variables, index, status):
        self.variables = variables
        self.site_id = index
        self.status = status

    @gen.coroutine
    def get(self):
        if self.status = SiteStatus.UP:
            self.set_status(200)
            self.write("Hello from Site %s" % (self.site_index))
        else:
            self.set_status(500)
