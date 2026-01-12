import abc
import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .autoscaling_actor_pool import AutoscalingActorPool


class ActorPoolResizingPolicy(abc.ABC):
    """Interface for determining how to resize an actor pool.

    When the actor pool needs to scale up, `compute_upscale_delta` will be called
    to determine how many new actors to add.
    When the actor pool needs to scale down, `compute_downscale_delta` will be called
    to determine how many actors to remove.
    """

    @abc.abstractmethod
    def compute_upscale_delta(
        self, actor_pool: "AutoscalingActorPool", util: float
    ) -> int:
        """Determine how many new actors to add when the actor pool needs to scale up.

        Args:
            actor_pool: The actor pool to scale.
            util: The current utilization of the actor pool.

        Returns:
            The number of actors to add (must be >= 1).
        """
        ...

    @abc.abstractmethod
    def compute_downscale_delta(self, actor_pool: "AutoscalingActorPool") -> int:
        """Determine how many actors to remove when the actor pool needs to scale down.

        Args:
            actor_pool: The actor pool to scale down.

        Returns:
            The number of actors to remove (must be >= 1).
        """
        ...


class DefaultResizingPolicy(ActorPoolResizingPolicy):
    """Policy that scales based on actor pool utilization.

    This policy calculates the upscale delta based on how much the current
    utilization exceeds the upscaling threshold. It always scales down by 1.
    """

    def __init__(
        self,
        upscaling_threshold: float,
        max_upscaling_delta: int,
    ):
        """Initialize the utilization-based resizing policy.

        Args:
            upscaling_threshold: The utilization threshold above which to scale up.
            max_upscaling_delta: The maximum number of actors to add in a single
                scale-up operation.
        """
        self._upscaling_threshold = upscaling_threshold
        self._max_upscaling_delta = max_upscaling_delta

    def compute_upscale_delta(
        self, actor_pool: "AutoscalingActorPool", util: float
    ) -> int:
        # Calculate desired delta based on utilization
        plan_delta = math.ceil(
            actor_pool.current_size() * (util / self._upscaling_threshold - 1)
        )

        # Apply limits
        limits = [
            self._max_upscaling_delta,
            actor_pool.max_size() - actor_pool.current_size(),
        ]
        delta = min(plan_delta, *limits)
        return max(1, delta)  # At least scale up by 1

    def compute_downscale_delta(self, actor_pool: "AutoscalingActorPool") -> int:
        return 1
