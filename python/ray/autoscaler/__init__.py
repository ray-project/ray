from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging


def _setup_logger():
    logger = logging.getLogger("ray.autoscaler")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
        ))
    logger.addHandler(handler)
    logger.propagate = False


_setup_logger()
