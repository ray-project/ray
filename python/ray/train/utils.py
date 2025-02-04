import warnings

from ray.util.annotations import RayDeprecationWarning


def _copy_doc(copy_func):
    def wrapped(func):
        func.__doc__ = copy_func.__doc__
        return func

    return wrapped


def _log_deprecation_warning(message):
    warnings.warn(
        message,
        RayDeprecationWarning,
        stacklevel=2,
    )
