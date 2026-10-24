"""Builders for the exact Ray resource request of a task or actor.

A resource request is a plain resource dict -- ``{"CPU": 1, "worker_group_a": 1}``
-- the same shape the autoscaling coordinator and Ray's autoscaler consume, so a
request can be forwarded to them without conversion. Custom resources are the
point: the logical ``ExecutionResources`` Ray Data tracks per task cannot express
them.
"""

from typing import Any, Dict

from ray._common.utils import resources_from_ray_options
from ray._private.ray_constants import (
    DEFAULT_ACTOR_CREATION_CPU_SIMPLE,
    DEFAULT_ACTOR_CREATION_CPU_SPECIFIED,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources


def task_resource_dict(options: Dict[str, Any]) -> Dict[str, float]:
    """Build the resource request for a regular Ray task.

    Args:
        options: The task's ``ray_remote_args``.

    Returns:
        The task's resource request, with zero-valued entries dropped. The dict
        is freshly built, so mutating it never touches ``options``.
    """
    resources = resources_from_ray_options(options)
    return {key: value for key, value in resources.items() if value > 0}


def actor_resource_dict(options: Dict[str, Any]) -> Dict[str, float]:
    """Build the resource request used to create a Ray actor.

    An actor with no resources reserves zero CPU for creation, while an actor
    with any non-memory resource reserves one CPU unless ``num_cpus`` is given.

    The branching rule mirrors ``ray.actor.ActorClass._remote()`` and shares its
    constants, but the *condition* is duplicated -- if Core changes when GPU
    implies CPU=1, update here too.

    Args:
        options: The actor's ``ray_remote_args``.

    Returns:
        The actor's resource request, with zero-valued entries dropped. The dict
        is freshly built, so mutating it never touches ``options``.
    """
    resources = resources_from_ray_options(options)
    if set(resources).difference({"memory", "object_store_memory"}):
        resources.setdefault("CPU", DEFAULT_ACTOR_CREATION_CPU_SPECIFIED)
    else:
        resources.setdefault("CPU", DEFAULT_ACTOR_CREATION_CPU_SIMPLE)
    return {key: value for key, value in resources.items() if value > 0}


def execution_resource_dict(resources: ExecutionResources) -> Dict[str, float]:
    """Build a resource request from the logical resources of a task.

    ``ExecutionResources`` cannot express custom resources, so operators that
    only carry a logical bundle lose them; they should report the request built
    from the task's remote options instead.

    Args:
        resources: The logical resources of a task or actor.

    Returns:
        The equivalent resource request, with zero-valued entries dropped.
    """
    return {
        key: value for key, value in resources.to_resource_dict().items() if value > 0
    }
