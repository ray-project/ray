from typing import Any

import ray

CACHED_FUNCTIONS = {}


def cached_remote_fn(fn: Any, **ray_remote_args) -> Any:
    """Lazily defines a ray.remote function.

    This is used in Datasets to avoid circular import issues with ray.remote.
    (ray imports ray.data in order to allow ``ray.data.read_foo()`` to work,
    which means ray.remote cannot be used top-level in ray.data).

    Note: Dynamic arguments should not be passed in directly,
    and should be set with ``options`` instead:
    ``cached_remote_fn(fn, **static_args).options(**dynamic_args)``.
    """
    if fn not in CACHED_FUNCTIONS:
        default_ray_remote_args = {
            "retry_exceptions": True,
            # Use the default scheduling strategy for all tasks so that we will
            # not inherit a placement group from the caller, if there is one.
            # The caller of this function may override the scheduling strategy
            # as needed.
            "scheduling_strategy": "DEFAULT",
        }
        CACHED_FUNCTIONS[fn] = ray.remote(
            **{**default_ray_remote_args, **ray_remote_args}
        )(fn)
    return CACHED_FUNCTIONS[fn]
