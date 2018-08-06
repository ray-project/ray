import logging
import sys

logger = logging.getLogger("ray.autoscaler")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.INFO)
logger.addHandler(ch)
