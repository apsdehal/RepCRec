import logging

from .config import config
from .SiteManager import SiteManager


class Main:
    def __init__(self):
        self.site_manager = SiteManager(config['NUM_SITES'])

    def run(self):
        self.site_manager.start()


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s',
                        level=config['LOG_LEVEL'])
    main = Main()
    main.run()
