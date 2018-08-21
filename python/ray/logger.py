from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys

default_logging_level = logging.INFO
default_logger = logging.getLogger("ray_default")
default_logger.setLevel(default_logging_level)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(default_logging_level)
default_logger.addHandler(ch)


def create_logger(level_threshold_str,
                  message_format=None,
                  date_format=None,
                  name=None):
    if message_format is None:
        formatter = None
    else:
        formatter = get_formatter(message_format, date_format)
    return get_logger_with_formatter(level_threshold_str, formatter, name)


def get_logger_with_formatter(level_threshold_str, formatter=None, name=None):
    if name is None:
        name = "formatted_ray_log"
    level_threshold = get_level_from_string(level_threshold_str)
    logger = logging.getLogger(name)
    logger.setLevel(level_threshold)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(level_threshold)
    # The user may only want to set the logging level.
    if isinstance(formatter, logging.Formatter):
        ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def get_level_from_string(level_string):
    level_string_upper = level_string.upper()
    level = logging.getLevelName(level_string_upper)
    if isinstance(level, str):
        default_logger.error("Unknown loggine level: {}".format(level_string))
        default_logger.error("Will use the default logging level: {}".format(
            logging.getLevelName(default_logging_level)))
        return default_logging_level
    else:
        return level


def get_formatter(message_format, date_format):
    return logging.Formatter(message_format, date_format)


def main():
    default_logger.debug("This is a debug log.")
    default_logger.info("This is a info log.")
    default_logger.warning("This is a warning log.")
    default_logger.error("This is a error log.")
    default_logger.critical("This is a critical log.")

    logger1 = get_logger_with_formatter("Nonexisting")
    logger1.debug("This is a debug log.")
    logger1.info("This is a info log.")
    logger1.warning("This is a warning log.")
    logger1.error("This is a error log.")
    logger1.critical("This is a critical log.")

    fmt = "%(asctime)s.%(msecs)d %(levelname)s - %(filename)s - %(message)s"
    datefmt = "%Y-%m-%d_%H-%M-%S"
    logger2 = create_logger("Error", fmt, datefmt, "logger.py-test")
    logger2.debug("This is a debug log.")
    logger2.info("This is a info log.")
    logger2.warning("This is a warning log.")
    logger2.error("This is a error log.")
    logger2.critical("This is a critical log.")


if __name__ == "__main__":
    main()
