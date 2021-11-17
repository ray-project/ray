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
        default_ray_remote_args = {
            "retry_exceptions": True,
            "placement_group": None,
        }
        CACHED_FUNCTIONS[fn] = ray.remote(**{
            **default_ray_remote_args,
            **ray_remote_args
        })(fn)
    return CACHED_FUNCTIONS[fn]
