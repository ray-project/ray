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
            # Use the default scheduling strategy for all tasks so that we will
            # not inherit a placement group from the caller, if there is one.
            # The caller of this function may override the scheduling strategy
            # as needed.
            "scheduling_strategy": "DEFAULT",
            "max_retries": -1,
        }
        ray_remote_args = {**default_ray_remote_args, **ray_remote_args}
        _add_system_error_to_retry_exceptions(ray_remote_args)
        CACHED_FUNCTIONS[fn] = ray.remote(**ray_remote_args)(fn)
    return CACHED_FUNCTIONS[fn]


def _add_system_error_to_retry_exceptions(ray_remote_args) -> None:
    """Modify the remote args so that Ray retries `RaySystemError`s.

    Ray typically automatically retries system errors. However, in some cases, Ray won't
    retry system errors if they're raised from task code. To ensure that Ray Data is
    fault tolerant to those errors, we need to add `RaySystemError` to the
    `retry_exceptions` list.

    TODO: Fix this in Ray Core. See https://github.com/ray-project/ray/pull/45079.
    """
    retry_exceptions = ray_remote_args.get("retry_exceptions", False)
    assert isinstance(retry_exceptions, (list, bool))

    if (
        isinstance(retry_exceptions, list)
        and ray.exceptions.RaySystemError not in retry_exceptions
    ):
        retry_exceptions.append(ray.exceptions.RaySystemError)
    elif not retry_exceptions:
        retry_exceptions = [ray.exceptions.RaySystemError]

    ray_remote_args["retry_exceptions"] = retry_exceptions
