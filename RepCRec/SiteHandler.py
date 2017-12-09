"""
Authors:
Amanpreet Singh
"""
from tornado import gen, web

from .enums.SiteStatus import SiteStatus
import json


class SiteHandler(web.RequestHandler):
    """
    Web request handler for a site.

    Args:
        variables: dict containing value of the variables
                   present on the site
        index: index of the site whose this handler is
        status: Tells whether is site is up and running
    """

    def initialize(self, variables, index, status):
        self.variables = variables
        self.site_index = index
        self.status = status

    @gen.coroutine
    def get(self):
        """
        Handles get request to this site.
        Dumps variable and their values if up
        Otherwise returns 500
        """
        string_vars = self.get_string_variables()
        if self.status == SiteStatus.UP:
            self.set_status(200)
            self.write("Hello from Site %s! \nMy variables in json:\n%s" %
                       (self.site_index, string_vars))
        else:
            self.set_status(500)

    def get_string_variables(self):
        """
        Utility function to get json representation of the variable values

        Returns:
            string containing json representation
        """
        ret = dict()
        for variable in self.variables:
            ret[variable] = self.variables[variable].value

        return json.dumps(ret, indent=4)
