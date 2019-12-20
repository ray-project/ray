from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging


logger = logging.getLogger(__name__)


def renamed_class(cls, old_name):
    """Helper class for renaming classes with a warning."""

    class DeprecationWrapper(cls):
        # note: **kw not supported for ray.remote classes
        def __init__(self, *args, **kw):
            new_name = cls.__module__ + "." + cls.__name__
            logger.warning("DeprecationWarning: {} has been renamed to {}. ".
                           format(old_name, new_name) +
                           "This will raise an error in the future.")
            cls.__init__(self, *args, **kw)

    DeprecationWrapper.__name__ = cls.__name__

    return DeprecationWrapper


def renamed_agent(cls):
    """Helper class for renaming Agent => Trainer with a warning."""

    class DeprecationWrapper(cls):
        def __init__(self, config=None, env=None, logger_creator=None):
            old_name = cls.__name__.replace("Trainer", "Agent")
            new_name = cls.__module__ + "." + cls.__name__
            logger.warning("DeprecationWarning: {} has been renamed to {}. ".
                           format(old_name, new_name) +
                           "This will raise an error in the future.")
            cls.__init__(self, config, env, logger_creator)

    DeprecationWrapper.__name__ = cls.__name__

    return DeprecationWrapper


def renamed_function(func, old_name):
    """Helper function for renaming a function."""

    def deprecation_wrapper(*args, **kwargs):
        new_name = func.__module__ + "." + func.__name__
        logger.warning(
            "DeprecationWarning: {} has been renamed to {}. This will raise an error in the future.".
            format(old_name, new_name)
        )
        return func(*args, **kwargs)

    deprecation_wrapper.__name__ = func.__name__

    return deprecation_wrapper


def moved_function(func):
    new_location = func.__module__
    logger.warning("DeprecationWarning: Function `{}}` has moved. Import from {}!".format(func.__name__, new_location))
    return func

