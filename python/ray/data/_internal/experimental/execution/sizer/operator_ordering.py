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

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.common import NodeIdStr
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

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
    # How many nodes the op can be scheduled on this tick. None = unknown.
    num_schedulable_nodes: Optional[int] = None

    @classmethod
    def from_operator(
        cls,
        op: "ExperimentalActorPoolMapOperator",
        *,
        topo_index: int,
        num_schedulable_nodes: Optional[int] = None,
    ) -> "OpOrderingSignals":
        """Derive the sort signals from an operator, so all knowledge of what
        the tiers mean lives in this module."""
        pool = op.actor_pool
        constraint = op.placement_constraint()
        return cls(
            topo_index=topo_index,
            has_placement_group=constraint.has_placement_group,
            has_label_selector=constraint.has_label_selector,
            is_gpu=pool.per_actor_resource_usage().gpu > 0,
            is_fixed_concurrency=pool.min_size() == pool.max_size(),
            num_schedulable_nodes=num_schedulable_nodes,
        )


class ScarcityFirstOrdering:
    """Scarcest / most-constrained operators first, sink-most within a tier.

    An unconstrained (or abundant-resource) op must not camp on the scarce
    nodes a constrained op needs. Tiers (lowest first): placement group,
    label selector, GPU, fixed concurrency, everything else. An op takes its
    lowest tier.
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
        return 4

    def order(self, signals: Dict[OpKey, OpOrderingSignals]) -> List[OpKey]:
        """Return the operator keys in processing order (first processed first)."""

        def key(op: OpKey) -> Tuple[int, float, int]:
            op_signals = signals[op]
            # Within a tier, fewer schedulable nodes goes first: an op that
            # fits on 1 node must be placed before one that fits on 5, or the
            # wider op can take the narrow op's only node.
            if op_signals.num_schedulable_nodes is None:
                return (self._tier(op_signals), float("inf"), -op_signals.topo_index)
            return (
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
            num_schedulable_nodes=(len(schedulable[op]) if op in schedulable else None),
        )
        for op in ops
        if op in topo_index_of
    }
    return _SCARCITY_FIRST.order(signals)
