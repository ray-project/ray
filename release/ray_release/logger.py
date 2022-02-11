import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        fmt="[%(levelname)s %(asctime)s] " "%(filename)s: %(lineno)d  " "%(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
