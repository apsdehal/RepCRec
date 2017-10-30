from tornado import gen, web


class SiteHandler(web.RequestHandler):
    def initialize(self, variables):
        self.variables = variables

    @gen.coroutine
    def get(self):
        self.write("Hello")
