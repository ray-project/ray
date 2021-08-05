from typing import Any

import ray

CACHED_FUNCTIONS = {}


def cached_remote_fn(fn: Any, **ray_remote_args) -> Any:
    """Lazily defines a ray.remote function.

    This is used in Datasets to avoid circular import issues with ray.remote.
    (ray imports ray.data in order to allow ``ray.data.read_foo()`` to work,
    which means ray.remote cannot be used top-level in ray.data).
    """
    if fn not in CACHED_FUNCTIONS:
        if ray_remote_args:
            CACHED_FUNCTIONS[fn] = ray.remote(**ray_remote_args)(fn)
        else:
            CACHED_FUNCTIONS[fn] = ray.remote(fn)
    return CACHED_FUNCTIONS[fn]
