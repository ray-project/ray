import logging

logger = logging.getLogger(__name__)

# A constant to use for any configuration that should be deprecated
# (to check, whether this config has actually been assigned a proper value or
# not).
DEPRECATED_VALUE = -1


def deprecation_warning(old, new=None, error=None):
    """
    Logs (via the `logger` object) or throws a deprecation warning/error.

    Args:
        old (str): A description of the "thing" that is to be deprecated.
        new (Optional[str]): A description of the new "thing" that replaces it.
        error (Optional[bool,Exception]): Whether or which exception to throw.
            If True, throw ValueError.
    """
    msg = "`{}` has been deprecated.{}".format(
        old, (" Use `{}` instead.".format(new) if new else ""))

    if error is True:
        raise ValueError(msg)
    elif error and issubclass(error, Exception):
        raise error(msg)
    else:
        logger.warning("DeprecationWarning: " + msg +
                       " This will raise an error in the future!")


def renamed_class(cls, old_name):
    """Helper class for renaming classes with a warning."""

    class DeprecationWrapper(cls):
        # note: **kw not supported for ray.remote classes
        def __init__(self, *args, **kw):
            new_name = cls.__module__ + "." + cls.__name__
            deprecation_warning(old_name, new_name)
            cls.__init__(self, *args, **kw)

    DeprecationWrapper.__name__ = cls.__name__

    return DeprecationWrapper


def renamed_agent(cls):
    """Helper class for renaming Agent => Trainer with a warning."""

    class DeprecationWrapper(cls):
        def __init__(self, config=None, env=None, logger_creator=None):
            old_name = cls.__name__.replace("Trainer", "Agent")
            new_name = cls.__module__ + "." + cls.__name__
            deprecation_warning(old_name, new_name)
            cls.__init__(self, config, env, logger_creator)

    DeprecationWrapper.__name__ = cls.__name__

    return DeprecationWrapper


def renamed_function(func, old_name):
    """Helper function for renaming a function."""

    def deprecation_wrapper(*args, **kwargs):
        new_name = func.__module__ + "." + func.__name__
        deprecation_warning(old_name, new_name)
        return func(*args, **kwargs)

    deprecation_wrapper.__name__ = func.__name__

    return deprecation_wrapper


def moved_function(func):
    new_location = func.__module__
    deprecation_warning("import {}".format(func.__name__),
                        "import {}".format(new_location))
    return func
