from enum import Enum, auto
from typing import Any

import ray

CACHED_FUNCTIONS = {}


class RemoteFnType(Enum):
    READ_WRITE = auto()  # Tasks for reading/writing data
    TASK = auto()  # Regular tasks
    ACTOR = auto()  # Actor initialization
    ACTOR_TASK = auto()  # Actor tasks


# Default and override configurations
DEFAULT_REMOTE_ARGS = {
    # Default remote args for read and write tasks.
    # Retry exceptions by default to handle transient errors
    # (such as S3 connection failures).
    RemoteFnType.READ_WRITE: {
        "retry_exceptions": True,
        # Use the default scheduling strategy for all tasks so that we will
        # not inherit a placement group from the caller, if there is one.
        # The caller of this function may override the scheduling strategy
        # as needed.
        "scheduling_strategy": "DEFAULT",
    },
    RemoteFnType.TASK: {},
    RemoteFnType.ACTOR: {},
    RemoteFnType.ACTOR_TASK: {},
}


def _get_default_args_for_task_type(remote_fn_type: RemoteFnType):
    default_args = DEFAULT_REMOTE_ARGS.get(task_type, {}).copy()

    # Get override arguments and update the defaults
    override_args_attr = f"{task_type.name}_REMOTE_ARGS"
    override_args = getattr(
        ray._internal._override_ray_remote_args, override_args_attr, {}
    )
    default_args.update(override_args)

    return default_args


def cached_remote_fn(fn: Any, remote_fn_type: RemoteFnType, **ray_remote_args) -> Any:
    """Lazily defines a ray.remote function.

    This is used in Datasets to avoid circular import issues with ray.remote.
    (ray imports ray.data in order to allow ``ray.data.read_foo()`` to work,
    which means ray.remote cannot be used top-level in ray.data).

    Note: Dynamic arguments should not be passed in directly,
    and should be set with ``options`` instead:
    ``cached_remote_fn(fn, **static_args).options(**dynamic_args)``.

    Args:
        fn: The function to cache.
        remote_fn_type: The type of fn to determine what default remote option
            arguments should be specified.
    """
    if fn not in CACHED_FUNCTIONS:
        default_ray_remote_args = _get_default_args_for_task_type(
            remote_fn_type=remote_fn_type
        )
        default_ray_remote_args.update(ray_remote_args)
        CACHED_FUNCTIONS[fn] = ray.remote(**default_ray_remote_args)(fn)
    return CACHED_FUNCTIONS[fn]
