"""The order the sizer grants and places operators in."""

from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Dict,
    Hashable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

from ray.data._internal.util import GiB

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.common import NodeIdStr
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

# Assumed physical memory per CPU core (bytes), ~4 GiB: the common
# hyperscaler ratio. An op whose per-actor memory request exceeds this times
# its CPUs is memory-bound for ordering (``is_large_memory``). Ops with the
# default memory reservation (4 GiB per CPU minus the system's share) are not.
MEMORY_PER_CPU_BYTES: int = 4 * GiB

OpKey = TypeVar("OpKey", bound=Hashable)


@dataclass(frozen=True)
class OpOrderingSignals:
    """Per-operator inputs the ordering sorts on."""

    # Position in source-first topology order; larger = closer to the sink.
    topo_index: int
    has_placement_group: bool
    has_label_selector: bool
    is_gpu: bool
    is_fixed_concurrency: bool
    # Memory request large relative to the actor's CPUs (memory-bound).
    is_large_memory: bool = False
    # Serving fewer actors than its min_size: the pipeline cannot run until it
    # gets them, so it outranks every other tier.
    below_min_size: bool = False
    # How many nodes the op can be scheduled on this tick. 0 before the first
    # tick snapshot (initial sizing), where every op ties.
    num_schedulable_nodes: int = 0

    @classmethod
    def from_operator(
        cls,
        op: "ExperimentalActorPoolMapOperator",
        *,
        topo_index: int,
        num_schedulable_nodes: int = 0,
    ) -> "OpOrderingSignals":
        """Derive the sort signals from an operator, so all knowledge of what
        the tiers mean lives in this module."""
        pool = op.actor_pool
        constraint = op.placement_constraint()
        per_actor = pool.per_actor_resource_usage()
        return cls(
            topo_index=topo_index,
            has_placement_group=constraint.has_placement_group,
            has_label_selector=constraint.has_label_selector,
            is_gpu=per_actor.gpu > 0,
            is_fixed_concurrency=pool.min_size() == pool.max_size(),
            is_large_memory=per_actor.memory > per_actor.cpu * MEMORY_PER_CPU_BYTES,
            below_min_size=pool.serving_size() < pool.min_size(),
            num_schedulable_nodes=num_schedulable_nodes,
        )


class ScarcityFirstOrdering:
    """Scarcest / most-constrained operators first, sink-most within a tier.

    An unconstrained (or abundant-resource) op must not camp on the scarce
    nodes a constrained op needs. Tiers (lowest first): placement group,
    label selector, GPU, fixed concurrency, large memory, everything else. An
    op takes its lowest tier.

    Ops below min_size go ahead of all others, since until every op has its
    minimum the pipeline cannot run end to end. Within that group the tiers
    still apply: at initial sizing every op is below min, and an unconstrained
    op placed first would camp on a constrained op's nodes.
    """

    @staticmethod
    def _tier(signals: OpOrderingSignals) -> int:
        if signals.has_placement_group:
            return 0
        if signals.has_label_selector:
            return 1
        if signals.is_gpu:
            return 2
        if signals.is_fixed_concurrency:
            return 3
        if signals.is_large_memory:
            return 4
        return 5

    def order(self, signals: Dict[OpKey, OpOrderingSignals]) -> List[OpKey]:
        """Return the operator keys in processing order (first processed first)."""

        def key(op: OpKey) -> Tuple[bool, int, int, int]:
            op_signals = signals[op]
            # Within a tier, fewer schedulable nodes goes first: an op that
            # fits on 1 node must be placed before one that fits on 5, or the
            # wider op can take the narrow op's only node.
            return (
                not op_signals.below_min_size,
                self._tier(op_signals),
                op_signals.num_schedulable_nodes,
                -op_signals.topo_index,
            )

        return sorted(signals, key=key)


_SCARCITY_FIRST = ScarcityFirstOrdering()


def ordered_actor_pool_ops(
    topology: "Topology",
    ops: Sequence["ExperimentalActorPoolMapOperator"],
    schedulable_nodes_by_op: Optional[
        Dict["ExperimentalActorPoolMapOperator", AbstractSet["NodeIdStr"]]
    ] = None,
) -> List["ExperimentalActorPoolMapOperator"]:
    """Return the operators in the order the sizer should grant/scale them."""
    topo_index_of = {op: topo_index for topo_index, op in enumerate(topology)}
    schedulable = schedulable_nodes_by_op or {}
    signals = {
        op: OpOrderingSignals.from_operator(
            op,
            topo_index=topo_index_of[op],
            num_schedulable_nodes=len(schedulable.get(op, ())),
        )
        for op in ops
        if op in topo_index_of
    }
    return _SCARCITY_FIRST.order(signals)
