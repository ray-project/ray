import warnings

from ray.util.annotations import RayDeprecationWarning


def _copy_doc(copy_func):
    def wrapped(func):
        func.__doc__ = copy_func.__doc__
        return func

    return wrapped


def _log_deprecation_warning(message: str):
    warnings.warn(
        message,
        RayDeprecationWarning,
        stacklevel=2,
    )


def _in_ray_train_worker() -> bool:
    from ray.train.v2._internal.constants import is_v2_enabled

    if is_v2_enabled():
        from ray.train.v2._internal.util import (
            _in_ray_train_worker as _in_ray_train_v2_worker,
        )

        return _in_ray_train_v2_worker()
    else:
        from ray.train._internal.session import (
            _in_ray_train_worker as _in_ray_train_v1_worker,
        )

        return _in_ray_train_v1_worker()
