from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from logging import FileHandler

import ray.ray_constants as ray_constants


logger = logging.getLogger(__name__)
# TODO: Make this logfile adjustable.
file_handler_info = FileHandler("test.log", mode="w")
file_handler_info.setLevel(logging.DEBUG)
file_handler_info.setFormatter(
        logging.Formatter(ray_constants.LOGGER_FORMAT))
logger.addHandler(file_handler_info)
