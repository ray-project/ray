""" Utilities to aid in building SAC implementation """
import logging
from functools import wraps

import ray

logger = logging.getLogger(__name__)


def using_ray_8():
    """Returns True if ray version is 0.8.1"""
    return ray.__version__ == '0.8.1'


def ray_8_only(func):
    """Decorator that runs the wrapped function only when using ray version 0.8.1"""
    @wraps(func)
    def wrapped(*args, **kwargs):
        if using_ray_8():
            return func(*args, **kwargs)
        else:
            logger.info(f"Not running {func.__name__} because ray version is not 0.8.1")

    return wrapped


def using_ray_6():
    """Returns True if ray version is 0.8.1"""
    return ray.__version__ == '0.6.6'
