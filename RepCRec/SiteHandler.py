from tornado import gen, web

from .enums.SiteStatus import SiteStatus
import json


class SiteHandler(web.RequestHandler):

    def initialize(self, variables, index, status):
        self.variables = variables
        self.site_index = index
        self.status = status

    @gen.coroutine
    def get(self):
        string_vars = self.get_string_variables()
        if self.status == SiteStatus.UP:
            self.set_status(200)
            self.write("Hello from Site %s! \nMy variables in json:\n%s" %
                       (self.site_index, string_vars))
        else:
            self.set_status(500)

    def get_string_variables(self):
        ret = dict()
        for variable in self.variables:
            ret[variable] = self.variables[variable].value

        return json.dumps(ret, indent=4)
