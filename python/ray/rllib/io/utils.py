from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import threading

logger = logging.getLogger(__name__)


class BlackHoleConsumer(threading.Thread):
    """Reads from a sampler and discards all the data."""

    def __init__(self, sampler):
        threading.Thread.__init__(self)
        self.sampler = sampler
        self.daemon = True

    def run(self):
        while True:
            try:
                self.sampler.get_data()
            except Exception:
                logger.exception("Error reading data")
