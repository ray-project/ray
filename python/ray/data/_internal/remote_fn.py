import functools
from typing import Any

import ray

from ray.data.context import DatasetContext

CACHED_FUNCTIONS = {}


def cached_remote_fn(fn: Any, **ray_remote_args) -> Any:
    """Lazily defines a ray.remote function.

    This is used in Datasets to avoid circular import issues with ray.remote.
    (ray imports ray.data in order to allow ``ray.data.read_foo()`` to work,
    which means ray.remote cannot be used top-level in ray.data).
    """
    if fn not in CACHED_FUNCTIONS:
        ctx = DatasetContext.get_current()
        default_ray_remote_args = {
            "retry_exceptions": True,
            "scheduling_strategy": ctx.scheduling_strategy,
        }

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            # Wrapper that sets the DatasetContext that existed on the driver/task
            # submitter.
            DatasetContext._set_current(ctx)
            return fn(*args, **kwargs)

        CACHED_FUNCTIONS[fn] = ray.remote(
            **{**default_ray_remote_args, **ray_remote_args}
        )(wrapper)
    return CACHED_FUNCTIONS[fn]
