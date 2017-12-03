import logging
import plac
import plac
from pathlib import Path

from .config import config
from .SiteManager import SiteManager
from .TransactionManager import TransactionManager
from .IO import IO
from .LockTable import LockTable


class Main:

    @plac.annotations(
        file_path=("File name", "positional", None, str),
        num_sites=("Number of Sites", "option", "n", int),
        num_variables=("Number of variables", "option", "v", int))
    def __init__(self, file_path,
                 num_sites=config['NUM_SITES'],
                 num_variables=config['NUM_VARIABLES']):

        p = Path('.')
        p = p / file_path

        self.site_manager = SiteManager(num_sites, num_variables)
        self.lock_table = LockTable()

        self.transaction_manager = TransactionManager(
            num_variables, num_sites, self.lock_table, self.site_manager)

        self.io = IO(p, self.site_manager,
                     self.transaction_manager, self.lock_table)

    def run(self):
        # self.site_manager.start()
        self.io.run()


if __name__ == "__main__":

    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s',
                        level=config['LOG_LEVEL'])

    main = plac.call(Main)
    main.run()
