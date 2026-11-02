"""Build the per-node snapshot of one operator that placement decides from."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Mapping, Tuple

from ray.data._internal.execution.interfaces import NodeIdStr
from ray.data._internal.execution.interfaces.common import ActorId, ActorStatus
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
    next_actor_pool_ops,
    prev_actor_pool_ops,
)
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    OpPlacementView,
)
from ray.data.context import DEFAULT_TARGET_MAX_BLOCK_SIZE

if TYPE_CHECKING:
    from collections import Counter

    from ray.data._internal.experimental.execution.operators.actor_pool import (
        _ExperimentalActorState,
    )


def build_op_placement_view(
    op: ExperimentalActorPoolMapOperator, *, rank_victims: bool
) -> OpPlacementView:
    """Where ``op``'s actors, queued input and neighbours are, per node.

    Args:
        op: The operator to snapshot.
        rank_victims: Also list each node's non-draining actors, cheapest to
            drain first (``victim_ids_by_node`` / ``victim_drain_cost_by_node``).
            Only downscale victim selection reads them; upscale placement
            needs counts only, so it passes False and skips the sort.

    Returns:
        The operator's placement snapshot for this tick.
    """
    pool = op.actor_pool
    victim_ids_by_node: Dict[NodeIdStr, List[ActorId]] = {}
    victim_drain_cost_by_node: Dict[NodeIdStr, List[Tuple[int, int]]] = {}
    actors_by_node: Dict[NodeIdStr, int] = {}
    idle_actors_by_node: Dict[NodeIdStr, int] = {}
    for node_id, states in pool.alive_actor_states_by_node().items():
        non_draining = {
            actor_id: state
            for actor_id, state in states.items()
            if state.status != ActorStatus.DRAINING
        }
        if not non_draining:
            continue
        actors_by_node[node_id] = len(non_draining)
        num_idle = sum(1 for s in non_draining.values() if s.num_tasks_in_flight == 0)
        if num_idle:
            idle_actors_by_node[node_id] = num_idle
        if rank_victims:
            ids, costs = _rank_victims(op, non_draining)
            victim_ids_by_node[node_id] = ids
            victim_drain_cost_by_node[node_id] = costs

    pending_ids_by_node = pool.pending_ids_by_target_node()
    for node_id, pending_ids in pending_ids_by_node.items():
        actors_by_node[node_id] = actors_by_node.get(node_id, 0) + len(pending_ids)

    # Neighbours' actor counts per node. Only actor-pool neighbours count: an
    # op with no actor-pool producer has nothing to collocate with upstream,
    # which is what makes it a source for placement purposes.
    upstream_ops = prev_actor_pool_ops(op)
    downstream_ops = next_actor_pool_ops(op)

    # Weight upstream vs downstream neighbours by each edge's measured share
    # of the op's byte flow (collocating near producers saves input bytes on
    # the wire; near consumers, output bytes). Neutral 0.5/0.5 (no bytes
    # observed yet) reproduces the unweighted rank.
    up_w = down_w = 0.5
    bytes_in = op.metrics.bytes_inputs_received
    bytes_out = op.metrics.bytes_task_outputs_generated
    total = bytes_in + bytes_out
    if total > 0:
        up_w = bytes_in / total
        down_w = bytes_out / total

    return OpPlacementView(
        op_id=op.id,
        is_source=not upstream_ops,
        per_actor_usage=pool.per_actor_resource_usage(),
        # Admission is by tasks in flight; an actor holds up to that many
        # ~target-size blocks, which is the bytes figure placement uses to
        # turn a node's queued input into a collocation fill.
        input_bytes_per_actor=pool.max_tasks_in_flight_per_actor()
        * (op.data_context.target_max_block_size or DEFAULT_TARGET_MAX_BLOCK_SIZE),
        actors_by_node=actors_by_node,
        input_queue_bytes_by_node=op.input_queue_bytes_by_node(),
        upstream_actors_by_node=_non_draining_count_by_node(upstream_ops),
        downstream_actors_by_node=_non_draining_count_by_node(downstream_ops),
        victim_ids_by_node=victim_ids_by_node,
        victim_drain_cost_by_node=victim_drain_cost_by_node,
        idle_actors_by_node=idle_actors_by_node,
        pending_ids_by_node=pending_ids_by_node,
        untargeted_pending_ids=pool.untargeted_pending_ids(),
        upstream_edge_weight=up_w,
        downstream_edge_weight=down_w,
    )


def _rank_victims(
    op: ExperimentalActorPoolMapOperator,
    states: Mapping[ActorId, "_ExperimentalActorState"],
) -> Tuple[List[ActorId], List[Tuple[int, int]]]:
    """One node's actors, cheapest to drain first, with their drain cost.

    Order: fewest tasks in flight, then fewest output blocks the ResourceBank
    still counts against the actor (both must reach 0 before a draining actor
    can exit), then least recently used. Returns ``(actor_ids, costs)`` with
    ``costs[i] = (tasks_in_flight, unconsumed_outputs)``.
    """
    ranked = sorted(
        (
            state.num_tasks_in_flight,
            op.resource_bank.live_object_store(actor_id=actor_id).get_output_blocks(),
            state.latest_task_submission_ts,
            actor_id,
        )
        for actor_id, state in states.items()
    )
    return (
        [actor_id for _, _, _, actor_id in ranked],
        [(in_flight, unconsumed) for in_flight, unconsumed, _, _ in ranked],
    )


def _non_draining_count_by_node(
    ops: Tuple[ExperimentalActorPoolMapOperator, ...],
) -> DefaultDict[NodeIdStr, int]:
    """Actors of ``ops`` per node, draining ones excluded."""
    counts: DefaultDict[NodeIdStr, int] = defaultdict(int)
    for neighbour in ops:
        for node_id, status_counts in neighbour.actor_counts_by_node().items():
            counts[node_id] += _non_draining_count(status_counts)
    return counts


def _non_draining_count(status_counts: "Counter[ActorStatus]") -> int:
    return sum(
        status_counts[status]
        for status in ActorStatus
        if status != ActorStatus.DRAINING
    )
