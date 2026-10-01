from dataclasses import dataclass
from typing import Any, Dict

from ray._common.utils import resources_from_ray_options
from ray._private.ray_constants import (
    DEFAULT_ACTOR_CREATION_CPU_SIMPLE,
    DEFAULT_ACTOR_CREATION_CPU_SPECIFIED,
)


@dataclass
class ResourceRequest:
    """The resource bundle for one task or actor.

    This intentionally represents the resource-related part of a Ray request.
    Other Ray scheduling options, such as placement groups and node affinity,
    are not part of the Ray Data autoscaler demand model.

    The resource mapping is copied on construction and remains mutable.
    """

    resources: Dict[str, float]

    def __post_init__(self):
        self.resources = dict(self.resources)

    @classmethod
    def from_task_options(cls, options: Dict[str, Any]) -> "ResourceRequest":
        """Build the resource request for a regular Ray task."""
        resources = resources_from_ray_options(options)
        return cls(
            resources={key: value for key, value in resources.items() if value > 0}
        )

    @classmethod
    def from_execution_resources(cls, resources: Any) -> "ResourceRequest":
        """Build a request from the base resources tracked by Ray Data."""
        return cls(
            resources={
                key: value
                for key, value in resources.to_resource_dict().items()
                if value > 0
            }
        )

    @classmethod
    def from_actor_options(cls, options: Dict[str, Any]) -> "ResourceRequest":
        """Build the resource request used to create a Ray actor.

        Actor creation has different CPU defaults from regular tasks. An actor
        with no resources reserves zero CPU for creation, while an actor with
        any non-memory resource reserves one CPU unless ``num_cpus`` is
        explicitly provided.

        The branching rule mirrors ``ray.actor.ActorClass._remote()`` and uses
        the same constants from ``ray._private.ray_constants``. The constants
        are shared so value changes propagate, but the *condition* is
        duplicated — if Core changes when GPU implies CPU=1, update here too.
        """
        resources = resources_from_ray_options(options)
        if set(resources).difference({"memory", "object_store_memory"}):
            resources.setdefault("CPU", DEFAULT_ACTOR_CREATION_CPU_SPECIFIED)
        else:
            resources.setdefault("CPU", DEFAULT_ACTOR_CREATION_CPU_SIMPLE)
        return cls(
            resources={key: value for key, value in resources.items() if value > 0},
        )
