"""One tick's capacity, spent down as operators are granted actors."""

from __future__ import annotations

import math
from typing import Dict, Hashable, List, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    max_placeable_actors,
    pack_actors,
)
from ray.data._internal.experimental.execution.sizer.models import TickCapacitySnapshot
from ray.data._internal.experimental.execution.sizer.pg_bundles import BundleKey

# Placeable count for an actor that requests no cpu/gpu/memory (fits anywhere).
_UNBOUNDED_PLACEABLE = 1 << 30


class ConstraintAwareCapacityLedger:
    """Free capacity for one tick's grants, spent down as ops are granted.

    Tracked per slot -- a node, or a placement-group bundle for PG ops --
    because an actor must fit inside one slot: two 3-CPU nodes hold two 2-CPU
    actors, not three. Each op only sees the slots it may run on, and grants
    run in placement order, so anything granted here is placeable.
    """

    def __init__(self, capacity: TickCapacitySnapshot):
        self._capacity = capacity
        self._free: Dict[NodeIdStr, ExecutionResources] = {
            node_id: res.copy() for node_id, res in capacity.free_by_node.items()
        }
        self._pg_free: Dict[BundleKey, ExecutionResources] = {
            key: res.copy() for key, res in capacity.pg_free_by_bundle.items()
        }
        # Ops whose PG capacity is unknown have no bundles to spend down.
        self._unknown_pg_headroom: Dict[ExperimentalActorPoolMapOperator, int] = dict(
            capacity.unknown_pg_headroom_by_op
        )

    def _schedulable_slots(
        self, op: ExperimentalActorPoolMapOperator
    ) -> Tuple[Dict, List[Hashable]]:
        """The free map ``op`` draws from, and its slots in fill order."""
        bundles = self._capacity.schedulable_bundles_by_op.get(op)
        if bundles is not None:
            return self._pg_free, list(bundles)
        # Least contended nodes first, so consuming them leaves contended
        # nodes for ops that need them.
        match = self._capacity.schedulable_nodes_by_op[op]
        pressure = self._capacity.constrained_op_count_by_node
        return self._free, sorted(
            (node_id for node_id in self._free if node_id in match),
            key=lambda node_id: (pressure.get(node_id, 0), node_id),
        )

    def max_upscale_delta(self, op: ExperimentalActorPoolMapOperator) -> int:
        """Max additional actors of ``op`` that currently fit (reclaimable
        draining actors included -- reclaiming them needs no new capacity)."""
        reclaimable = op.actor_pool.num_reclaimable_draining_actors()
        if op in self._unknown_pg_headroom:
            # Headroom is max_size - current_size, and current_size still
            # counts draining actors, so revivable ones come on top.
            return self._unknown_pg_headroom[op] + reclaimable
        per = self._capacity.per_actor_resources_by_op[op]
        free, slots = self._schedulable_slots(op)
        total = 0.0
        for slot in slots:
            count = max_placeable_actors(free[slot], per)
            if math.isinf(count):
                return _UNBOUNDED_PLACEABLE
            total += count
        # Draining actors this op can reclaim hold their (committed, i.e.
        # not-free) slot, so they place without consuming free capacity.
        return int(total) + reclaimable

    def grant(self, op: ExperimentalActorPoolMapOperator, count: int) -> None:
        """Debit a grant of ``count`` actors so later ops see reduced capacity.

        ``count`` is the full delta; only the genuinely-new actors beyond the
        op's reclaimable draining ones consume free capacity.
        """
        num_new = max(0, count - op.actor_pool.num_reclaimable_draining_actors())
        if op in self._unknown_pg_headroom:
            self._unknown_pg_headroom[op] -= min(num_new, self._unknown_pg_headroom[op])
            return
        free, slots = self._schedulable_slots(op)
        pack_actors(free, slots, self._capacity.per_actor_resources_by_op[op], num_new)
