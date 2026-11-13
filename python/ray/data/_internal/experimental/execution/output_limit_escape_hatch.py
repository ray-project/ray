"""Deadlock escape hatch for per-actor output limits.

The actor-only backend bounds how much output each actor may have outstanding
(``ExperimentalActorPoolMapOperator.task_pull_request``). A budget that is
exhausted is normally relieved by downstream consumption. It cannot be relieved
when the consumer needs MORE rows than the producer is allowed to have
outstanding before it can run at all -- an equal ``OutputSplitter`` waiting for
``num_output_splits`` bundles' worth of rows, a map waiting for its minimum bundle -- or when the
budget is too small to even cover the producer's already-prebuffered outputs.
Both are deadlocks that no amount of waiting fixes; the hatch raises exactly one
operator's output limit by one block's worth and lets the loop retry.

Lineage reconstruction adds a third: a re-made block is withheld until its
child's whole input set is back, charged to the actor that made it. If that
fills the actor's budget before the set is complete, the actor stops and the set
may never complete. ``relieve_withheld_deadlock`` runs first: only when actors
are provably deadlocked (see ``withheld_deadlock``) does it give room to the
one the others wait on.

Extra budget comes first from the node's free object store (reserved so no
other grant counts it again), then from other actors on the node, which lend
unused budget. Only when both fall short does a grant raise the node's total
past what it holds, accepting a spill over a stuck pipeline (see
``_ExperimentalActorPool.grant_output_headroom``).

The executor calls ``relieve_output_deadlock`` on an adaptive interval
(backing off while nothing is stuck, tightening after a hit) so the scan stays
cheap.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.resource_bank import ResourceBankBase
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)

__all__ = ["relieve_output_deadlock", "num_rows_required"]


def relieve_output_deadlock(
    *, output_operator: PhysicalOperator, resource_bank: ResourceBankBase
) -> Optional[PhysicalOperator]:
    """Break one deadlock that waiting can't resolve by raising an operator's
    output limit. Returns the relieved operator, or ``None``.

    Withheld-output deadlocks are checked first (``relieve_withheld_deadlock``).
    Otherwise this looks for a backpressure deadlock: an output-backpressured
    producer that cannot be relieved by waiting (``relieve_backpressure_deadlock``).
    Each operator records its own grant metric, by deadlock kind.

    Data flows producer -> consumer, so this walks upstream (DFS) from the
    output operator. Only an ``ExperimentalActorPoolMapOperator`` can be
    output-backpressured, so it is the only kind ever upgraded, and only when it
    is backpressured AND either

    (a) its consumer is starved -- the producer has fewer output rows pulled
        than the consumer needs to make progress -- and no downstream task is
        running that could free the producer's budget by consuming, or
    (b) the producer lacks budget even for its prebuffered outputs (a low
        object-store stall).

    At most one operator is relieved per call; ``relieve_backpressure_deadlock``
    itself is debounced, so a grant is only "spent" on a call that took effect.
    """
    # Stack entries: (op, downstream_rows_needed, downstream_has_active_tasks).
    # downstream_rows_needed: output rows ``op`` must supply before the data
    #   path below it can progress (e.g. an equal OutputSplitter needs
    #   ``num_output_splits`` rows to dispatch a balanced split).
    # downstream_has_active_tasks: a downstream task-launcher has tasks in
    #   flight that will consume refs and thereby free the producer's budget.
    for op in output_operator.post_order_iter():
        if (
            isinstance(op, ExperimentalActorPoolMapOperator)
            and not op.has_completed()
            and op.relieve_withheld_deadlock()
        ):
            return op

    stack: List[Tuple[PhysicalOperator, int, bool]] = [(output_operator, 0, False)]
    while stack:
        curr_op, downstream_rows_needed, downstream_has_active_tasks = stack.pop()
        if curr_op.has_completed():
            continue

        if isinstance(curr_op, ExperimentalActorPoolMapOperator):
            in_output_backpressure = curr_op.is_output_backpressured()
            # The producer's OWN output store holds the refs its consumer waits
            # on, so that is where starvation is measured. Withheld rows are
            # pulled but not queued, so the consumer can't count on them.
            producer_live = resource_bank.live_object_store(op=curr_op)
            pulled_output_rows = (
                producer_live.num_pulled_output_rows
                - producer_live.num_withheld_output_rows
            )
            consumer_starved = pulled_output_rows < downstream_rows_needed
            if in_output_backpressure and (
                (consumer_starved and not downstream_has_active_tasks)
                or not curr_op.has_enough_budget_for_prebuffered_outputs()
            ):
                if curr_op.relieve_backpressure_deadlock():
                    return curr_op

            # A task-launcher drains its producer: its producer only needs to
            # feed THIS operator enough rows to launch a task, and its active
            # tasks are what relieve its producer's backpressure.
            curr_has_active_tasks = curr_op.get_actor_info().active > 0
            curr_rows_needed = num_rows_required(curr_op)
        else:
            # A pass-through launches no tasks and routes refs unchanged, so it
            # forwards its downstream's requirement and active-task signal to
            # its own producer -- summed, so the requirement survives a chain of
            # pass-throughs (e.g. map -> limit -> OutputSplitter).
            curr_has_active_tasks = downstream_has_active_tasks
            curr_rows_needed = num_rows_required(curr_op) + downstream_rows_needed

        for producer in curr_op.input_dependencies:
            stack.append((producer, curr_rows_needed, curr_has_active_tasks))

    return None


def num_rows_required(op: PhysicalOperator) -> int:
    """Rows ``op`` needs queued before it can process an input. Queueing
    operators know their own requirement (a map's bundle target, an equal
    split's balance); everything else needs nothing."""
    if isinstance(op, InternalQueueOperatorMixin):
        return op.min_num_rows_needed_to_make_progress()
    return 0
