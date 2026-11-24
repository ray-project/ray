"""One tick's capacity, spent down as the policy decides."""

from __future__ import annotations

import math
from typing import Dict, Hashable, List, Sequence, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    BundleKey,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    max_placeable_actors,
    pack_actors,
)
from ray.data._internal.experimental.execution.sizer.models import ClusterStateSnapshot

# Placeable count for an actor that requests no cpu/gpu/memory (fits anywhere).
_UNBOUNDED_PLACEABLE = 1 << 30


class ConstraintAwareCapacityLedger:
    """Free capacity for one tick's decisions, spent down as the policy
    decides. Every rule, bottleneck or per-op, checks and spends capacity
    here, so no decision this tick hands out capacity another one took.

    Tracked per slot -- a node, or a placement-group bundle for PG ops --
    because an actor must fit inside one slot: two 3-CPU nodes hold two 2-CPU
    actors, not three. Each op only sees the slots it may run on.

    There are two ways to spend:

    - ``claim*``: the decision picks the nodes itself (the bottleneck model).
      The ledger debits its own maps and the snapshot's, so WHERE does not
      place anything else into that capacity.
    - ``grant``: the decision is only a count (a per-op rule). The ledger
      debits its own maps only; WHERE places the actors later into the
      snapshot's maps.
    """

    def __init__(self, capacity: ClusterStateSnapshot):
        self._capacity = capacity
        self.free: Dict[NodeIdStr, ExecutionResources] = {
            node_id: res.copy() for node_id, res in capacity.free_by_node.items()
        }
        self.pg_free: Dict[BundleKey, ExecutionResources] = {
            key: res.copy() for key, res in capacity.pg_free_by_bundle.items()
        }
        # Ops whose PG capacity is unknown have no bundles to spend down.
        self.unknown_pg_headroom: Dict[ExperimentalActorPoolMapOperator, int] = dict(
            capacity.unknown_pg_headroom_by_op
        )

    # --- reads ----------------------------------------------------------------

    def free_nodes(self, op: ExperimentalActorPoolMapOperator) -> List[NodeIdStr]:
        """Nodes with free capacity that ``op`` may run on."""
        schedulable = self._capacity.schedulable_nodes_by_op.get(op, frozenset())
        return [node for node in self.free if node in schedulable]

    def _schedulable_slots(
        self, op: ExperimentalActorPoolMapOperator
    ) -> Tuple[Dict, List[Hashable]]:
        """The free map ``op`` draws from, and its slots in fill order."""
        bundles = self._capacity.schedulable_bundles_by_op.get(op)
        if bundles is not None:
            return self.pg_free, [b for b in bundles if b in self.pg_free]
        # Least contended nodes first, so consuming them leaves contended
        # nodes for ops that need them.
        pressure = self._capacity.constrained_op_count_by_node
        return self.free, sorted(
            self.free_nodes(op),
            key=lambda node_id: (pressure.get(node_id, 0), node_id),
        )

    def placeable_new(self, op: ExperimentalActorPoolMapOperator) -> int:
        """New ``op`` actors that fit in its free slots, or in its PG
        headroom when its PG capacity is unknown. Counted per slot, so
        fragmented CPU/GPU/memory is not mistaken for placeable actors."""
        if op in self.unknown_pg_headroom:
            return max(0, self.unknown_pg_headroom[op])
        per = op.actor_pool.per_actor_resource_usage()
        free, slots = self._schedulable_slots(op)
        total = 0.0
        for slot in slots:
            count = max_placeable_actors(free[slot], per)
            if math.isinf(count):
                return _UNBOUNDED_PLACEABLE
            total += count
        return int(total)

    def max_upscale_delta(self, op: ExperimentalActorPoolMapOperator) -> int:
        """Max additional actors of ``op`` that currently fit. Draining actors
        it can reclaim count too: they still hold their slot, so they need no
        free capacity."""
        return self.placeable_new(op) + op.actor_pool.num_reclaimable_draining_actors()

    # --- spending -------------------------------------------------------------

    def claim(self, node: NodeIdStr, per_actor: ExecutionResources, count: int) -> None:
        """Hold room for ``count`` actors of ``per_actor`` on ``node``, as far
        as it goes (a transfer holds room for actors it places later)."""
        held = per_actor.scale(count)
        for free in (self.free, self._capacity.free_by_node):
            if node in free:
                free[node] = free[node].subtract(held).max(ExecutionResources.zero())

    def claim_on_nodes(
        self,
        op: ExperimentalActorPoolMapOperator,
        count: int,
        nodes: List[NodeIdStr],
    ) -> List[NodeIdStr]:
        """Place up to ``count`` new ``op`` actors on ``nodes``, in order.
        Returns one node per placed actor."""
        per = op.actor_pool.per_actor_resource_usage()
        placed = pack_actors(self.free, nodes, per, count)
        self._mirror(self._capacity.free_by_node, placed, per)
        return placed

    def claim_in_pg(self, op: ExperimentalActorPoolMapOperator, count: int) -> int:
        """How many of ``count`` new actors of a PG op fit in its bundles (or
        its headroom when its PG capacity is unknown), taken from them. Ray
        core picks the bundle, so only the count matters."""
        if op in self.unknown_pg_headroom:
            fit = max(0, min(count, self.unknown_pg_headroom[op]))
            self.unknown_pg_headroom[op] -= fit
            return fit
        per = op.actor_pool.per_actor_resource_usage()
        free, slots = self._schedulable_slots(op)
        placed = pack_actors(free, slots, per, count)
        self._mirror(self._capacity.pg_free_by_bundle, placed, per)
        return len(placed)

    def grant(self, op: ExperimentalActorPoolMapOperator, count: int) -> None:
        """Debit a grant of ``count`` actors so later decisions see less room.

        ``count`` is the full delta; only the new actors beyond the op's
        reclaimable draining ones use free capacity.
        """
        num_new = max(0, count - op.actor_pool.num_reclaimable_draining_actors())
        if op in self.unknown_pg_headroom:
            self.unknown_pg_headroom[op] -= min(num_new, self.unknown_pg_headroom[op])
            return
        free, slots = self._schedulable_slots(op)
        pack_actors(free, slots, op.actor_pool.per_actor_resource_usage(), num_new)

    @staticmethod
    def _mirror(
        free: Dict, placed: Sequence[Hashable], per_actor: ExecutionResources
    ) -> None:
        """Debit the same placements off the snapshot's map. It has at least
        as much room as the ledger's, which grants have also spent."""
        for slot in placed:
            if slot in free:
                free[slot] = (
                    free[slot].subtract(per_actor).max(ExecutionResources.zero())
                )
