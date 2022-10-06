# From https://docs.ray.io/en/latest/ray-overview/index.html

import ray
import logging
from logging import getLogger

ray.init()
print("Starting Ray job")


# Set up logger with timestamps
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = getLogger(__name__)
logger.info("Starting Ray job")


@ray.remote
def f(x):
    return x * x


futures = [f.remote(i) for i in range(4)]
print(ray.get(futures))  # [0, 1, 4, 9]


@ray.remote
class Counter(object):
    def __init__(self):
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(message)s",
            level=logging.INFO,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.n = 0

    def increment(self):
        logging.info("Incrementing counter")
        self.n += 1

    def read(self):
        logging.info("Reading counter")
        return self.n


logger.info("Instantiating Counter")
counters = [Counter.remote() for i in range(4)]
logger.info("Incrementing Counter")
[c.increment.remote() for c in counters]
logger.info("Reading Counter")
futures = [c.read.remote() for c in counters]
logger.info("Calling ray.get()")
print(ray.get(futures))  # [1, 1, 1, 1]
