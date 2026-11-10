"""Task-completion processing for the actor-only scheduling loop.

Differences from the standard ``process_completed_tasks``:

- Each ready data task is read up to the budget its operator quotes for it
  (``task_pull_request``: bytes AND blocks), instead of a per-operator byte
  budget handed down by backpressure policies. The actor-only loop consults no
  backpressure policies -- admission is owned by the per-actor budgets.
- An operator's actor-readiness ``MetadataOpTask``s are processed before its
  ``DataOpTask``s, so a block-level abort cannot leave pending actors stranded.
- Inputs are moved from an operator's inqueue into the operator eagerly here
  (the operator only enqueues; dispatch happens in the executor's launch phase).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import DefaultDict, Dict, List, Tuple

import ray
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    Waitable,
)
from ray.data._internal.execution.metadata_fetcher import MetadataFetcher
from ray.data._internal.execution.streaming_executor_state import (
    WAIT_FOR_TASK_COMPLETION_TIMEOUT_S,
    OpState,
    Topology,
    _handle_data_op_task_exception,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)

logger = logging.getLogger(__name__)

__all__ = ["process_completed_tasks"]


def process_completed_tasks(
    *,
    topology: Topology,
    max_errored_blocks: int,
    metadata_fetcher: MetadataFetcher,
) -> int:
    """Process newly completed tasks and move queued inputs into operators.
    Call ``update_operator_states()`` afterwards.

    Args:
        topology: The topology of operators.
        max_errored_blocks: Max number of errored blocks to allow, unlimited if
            negative.
        metadata_fetcher: Resolves pulled ``(block_ref, meta_ref)`` pairs into
            emitted RefBundles (inline, or deferred to a background thread).

    Returns:
        The number of errored blocks.
    """
    # All active tasks, keyed by their waitables.
    active_tasks: Dict[Waitable, Tuple[OpState, OpTask]] = {}
    for op, state in topology.items():
        for task in op.get_active_tasks():
            active_tasks[task.get_waitable()] = (state, task)

    num_errored_blocks = 0
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=WAIT_FOR_TASK_COMPLETION_TIMEOUT_S,
        )

        ready_tasks_by_op: DefaultDict[OpState, List[OpTask]] = defaultdict(list)
        for ref in ready:
            state, task = active_tasks[ref]
            ready_tasks_by_op[state].append(task)

        # Consumers first: their pulls free producer budget for this same tick.
        for op, state in reversed(topology.items()):
            ready_tasks = ready_tasks_by_op[state]
            if not ready_tasks:
                continue
            assert isinstance(op, ExperimentalActorPoolMapOperator), op
            op_data_tasks: List[DataOpTask] = []
            try:
                for task in ready_tasks:
                    if not isinstance(task, MetadataOpTask):
                        continue
                    try:
                        task.on_task_finished()
                    except Exception:
                        # The readiness probe of an actor that failed to START
                        # (e.g. its target node died). The pool has already
                        # dropped the pending entry; the sizer re-places on its
                        # next tick, so this must not abort the loop.
                        logger.debug(
                            f"An actor of {op.name} failed to start; "
                            "reconciling on the next sizing tick.",
                            exc_info=True,
                        )

                for task in ready_tasks:
                    if not isinstance(task, DataOpTask):
                        continue
                    try:
                        task.on_data_ready(
                            max_to_read=op.task_pull_request(
                                task_idx=task.task_index()
                            ),
                            metadata_fetcher=metadata_fetcher,
                        )
                        op_data_tasks.append(task)
                    except Exception as e:
                        num_errored_blocks = _handle_data_op_task_exception(
                            e, op.name, max_errored_blocks, num_errored_blocks
                        )
            finally:
                # Hand this op's deferred pairs to the fetcher and register
                # end-of-stream tasks for a postponed done-callback (a no-op in
                # inline mode). In a ``finally`` so a thrown error cannot strand
                # pairs already deferred this iteration.
                metadata_fetcher.submit(state, op_data_tasks)

    # Emit whatever is ready and fire postponed done callbacks --
    # unconditionally, even with no active tasks: pairs deferred in earlier
    # iterations can still have metadata land now, and gating on
    # ``active_tasks`` would strand them. Deferred fetch failures share the
    # ``max_errored_blocks`` accounting with inline errors.
    for (
        failed_op_name,
        fetch_exc,
    ) in metadata_fetcher.emit_ready_and_fire_done_callbacks():
        num_errored_blocks = _handle_data_op_task_exception(
            fetch_exc, failed_op_name, max_errored_blocks, num_errored_blocks
        )

    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())

        # A completed operator (e.g. an early-finishing limit) must not be fed
        # further inputs.
        if not op.has_completed():
            while op_state.has_pending_input_bundles():
                op_state.move_input_into_op()

    return num_errored_blocks
