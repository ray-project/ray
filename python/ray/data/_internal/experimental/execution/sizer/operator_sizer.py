"""The actor-only backend's operator sizer.

The sizer owns every actor-pool sizing decision under the actor-only backend:
initial pool sizes, per-tick up/downscale deltas, placement-resolved requests
(which node a new actor lands on, which actor drains -- see
``ExperimentalActorPoolScalingRequest``), and the per-tick pool upkeep of
draining and stuck-pending actors. Operators never size themselves:
``ExperimentalActorPoolMapOperator._scale_to_initial_size`` is a no-op for
exactly this reason.

``ExperimentalStreamingExecutor`` drives the sizer through the three entry
points below, in this order: ``bootstrap`` and ``initial_sizing_request`` once
before the scheduling loop starts, then ``perform_sizing_decision`` once per
scheduling-loop step (after completed tasks are processed, before new tasks are
launched). This module currently carries that contract only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
        ActorOnlyResourceReporter,
    )


class OperatorSizer:
    """Sizes the actor pools of one dataset execution.

    Reads this dataset's cluster allocation and per-node view through the
    ``ActorOnlyResourceReporter`` (the cluster autoscaler owns the underlying
    coordinator request's lifecycle, so the sizer never touches the coordinator
    directly).
    """

    def __init__(
        self, *, dataset_id: str, resource_reporter: "ActorOnlyResourceReporter"
    ):
        self._dataset_id = dataset_id
        self._resource_reporter = resource_reporter

    def bootstrap(self, *, topology: "Topology") -> None:
        """One-time setup against the started topology, before any sizing."""

    def initial_sizing_request(self, *, topology: "Topology") -> None:
        """Bring every actor pool to its initial size."""

    def perform_sizing_decision(self, *, topology: "Topology") -> None:
        """One sizing tick: observe, decide per-pool deltas, place them, apply
        them, and advance draining / stuck-pending actors on every pool."""
