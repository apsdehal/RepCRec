"""
Authors:
Amanpreet Singh
Sharan Agrawal
"""
import logging
import plac
import plac
from pathlib import Path

from .config import config
from .SiteManager import SiteManager
from .TransactionManager import TransactionManager
from .IO import IO
from .LockTable import LockTable
from tornado.ioloop import IOLoop
from tornado import gen


class Main:
    """
    Main class representing the whole project.
    Job is to start site manager, transaction manager and IO.

    Args:
        file_path: File containing instructions
        num_sites: Number of sites
        num_variables: Number of variables
        sites: Whether to bind sites to port
        out_file: If out_file is present, logs will be written to it
        stdin: Whether to take input from stdin
    """
    @plac.annotations(
        file_path=("File name, pass anything in case of stdin",
                   "positional", None, str),
        num_sites=("Number of Sites", "option", "n", int),
        num_variables=("Number of variables", "option", "v", int),
        sites=("Whether to bring sites up", "flag", "s"),
        out_file=("Output file, if not passed by default we" +
                  " will print to stdout", "option", "o", str),
        stdin=("Takes input from stdin instead of file if passed", "flag",
               "i"))
    def __init__(self, file_path,
                 num_sites=config['NUM_SITES'],
                 num_variables=config['NUM_VARIABLES'],
                 sites=False,
                 out_file=None,
                 stdin=False):
        p = Path('.')
        p = p / file_path

        if out_file:
            open(out_file, 'w').close()

        logging.basicConfig(filename=out_file,
                            format='%(levelname)s - %(asctime)s - %(message)s',
                            level=config['LOG_LEVEL'])

        self.site_manager = SiteManager(num_sites, num_variables)

        self.lock_table = LockTable()

        self.transaction_manager = TransactionManager(
            num_variables, num_sites, self.lock_table, self.site_manager)

        self.io = IO(p, self.site_manager,
                     self.transaction_manager, self.lock_table, stdin=stdin)
        self.sites = sites

    @gen.coroutine
    def run_coroutine(self):
        """
        Runs sites and binds them to port. Then, starts all the IO
        """
        if self.sites:
            self.site_manager.start()

        self.io.run()

    def run(self):
        """
        Directly starts IO
        """
        if self.sites:
            self.site_manager.start()

        self.io.run()


if __name__ == "__main__":
    main = plac.call(Main)

    if main.sites:
        IOLoop.instance().add_callback(main.run_coroutine)
        IOLoop.current().start()
    else:
        main.run()
