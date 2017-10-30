from tornado import gen, web


class SiteHandler(web.RequestHandler):
    def initialize(self, variables, index):
        self.variables = variables
        self.site_id = index

    @gen.coroutine
    def get(self):
        self.write("Hello from Site %s" % (self.site_index))
