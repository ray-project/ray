"""Per-operator metrics seam of the actor-only backend.

``_ActorOnlyOpMetrics`` is the structural protocol ``ExperimentalActorPoolMapOperator``
and ``_ExperimentalActorPool`` record into. The streaming executor injects the
concrete implementation through ``set_actor_only_metrics()``, which forwards it to
the pool; neither imports the concrete class and both run without one injected.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class _ActorOnlyOpMetrics(Protocol):
    """Overshoot and drain events recorded by the actor-only operator and pool."""

    def record_block_size_overshoot(self, op_tag: str) -> None:
        ...

    def record_output_limit_overshoot(self, op_tag: str) -> None:
        ...

    def record_drain_started(self, op_tag: str, reason: str) -> None:
        ...

    def record_drain_completed(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        ...

    def record_drain_interrupted(
        self, op_tag: str, reason: str, duration_s: float
    ) -> None:
        ...

    def record_drain_cancelled(self, op_tag: str, reason: str, cause: str) -> None:
        ...
