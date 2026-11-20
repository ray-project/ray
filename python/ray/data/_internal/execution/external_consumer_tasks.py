"""Track the lineage of external consumer tasks, which represent the tasks that
consume output blocks produced by the final operator.

An external consumer takes outputs from the executor and consumes them in tasks
that are submitted and monitored outside the executor's thread, such as the
push-based split delivering blocks to train workers. The consumer reports each
task's submission and outcome from its own threads. The executor applies those
reports to the lineage tracker once per scheduling step, on its own thread,
which is the only thread that touches the tracker.

Each external task is registered with the lineage tracker as a child of the
tasks that produced its blocks. If its input was lost, it is the target of a
reconstruction plan like a task inside the executor that hit a lost object. Only the blocks
it consumed are re-produced.
"""

import itertools
import threading
from typing import (
    TYPE_CHECKING,
    Dict,
    FrozenSet,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import ReconstructionStamp
from ray.exceptions import ObjectLostError
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data._internal.execution.lineage_tracker import LineageTracker

# Identifies one external task. Returned by ``ExternalConsumerTasks.submit`` and
# passed back to report the task's outcome.
ExternalTaskHandle = int


class ExternalTaskLostInput(NamedTuple):
    """An external task whose input was lost, to be reconstructed by the
    executor."""

    lineage_task_id: str
    # The plans the task served, or empty for a fresh task.
    reconstruction_plan_ids: FrozenSet[str]
    error: ObjectLostError


class ExternalConsumerTasks:
    """Records the external tasks that consumer threads report, and applies them
    to the lineage tracker on the executor thread.

    Every method is thread-safe except ``update_lineage`` and
    ``pop_lost_inputs``, which only the executor thread calls, in that order.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._next_handle = itertools.count()
        # Blocks the executor handed out that no external task was submitted
        # for yet, mapped to the output split that took them.
        self._outputs_awaiting_submission: Dict[ObjectRef, Optional[int]] = {}
        # Reports not yet registered. A task is always submitted before it can
        # complete, lose its input, or be abandoned, so registering every submission of a
        # drain before any outcome keeps them in order.
        self._submitted: List[
            Tuple[ExternalTaskHandle, ObjectRef, Optional[ReconstructionStamp]]
        ] = []
        self._completed: List[ExternalTaskHandle] = []
        self._lost_inputs: List[Tuple[ExternalTaskHandle, ObjectLostError]] = []
        self._abandoned: List[ExternalTaskHandle] = []
        # Tasks submitted but not yet completed, lost, or abandoned.
        self._num_unresolved = 0
        # Output splits whose consumer stopped early.
        self._finished_splits: Set[int] = set()

        # Executor thread only. The lineage task and reconstruction plans each
        # registered external task runs as.
        self._lineage_ids: Dict[ExternalTaskHandle, Tuple[str, FrozenSet[str]]] = {}
        # Executor thread only. Lost inputs set aside by `update_lineage` for
        # `pop_lost_inputs`.
        self._lost_inputs_to_pop: List[ExternalTaskLostInput] = []

    def on_output_taken(
        self, bundle: RefBundle, output_split_idx: Optional[int] = None
    ) -> None:
        """Called by the executor when an external consumer takes an output.

        Keeps the executor alive until an external task is submitted for each
        of the output's blocks, or until the split that took it finishes.

        Args:
            bundle: The output taken.
            output_split_idx: The output split that took it, if the executor's
                output is split.
        """
        # A re-produced bundle is the whole input of the one external task that
        # retries the failed one. More blocks would make several tasks claim
        # its lineage task.
        assert bundle.reconstruction_stamp is None or len(bundle.blocks) == 1, bundle
        with self._lock:
            # A split that already finished never submits a task for it.
            if output_split_idx in self._finished_splits:
                return
            for entry in bundle.blocks:
                self._outputs_awaiting_submission[entry.ref] = output_split_idx

    def submit(
        self, block_ref: ObjectRef, stamp: Optional[ReconstructionStamp]
    ) -> ExternalTaskHandle:
        """Submit an external task that consumes a block the executor handed out.

        Args:
            block_ref: The block the task consumes. It must have been handed out
                by the executor, and not consumed by another task yet.
            stamp: The ``reconstruction_stamp`` of the bundle the block came in.
                Set when the block is the re-produced input of a failed external
                task, which this task then retries.

        Returns:
            A handle to report the task's outcome with.
        """
        with self._lock:
            # Already dropped if its split finished while the task was being
            # submitted.
            self._outputs_awaiting_submission.pop(block_ref, None)
            handle = next(self._next_handle)
            self._submitted.append((handle, block_ref, stamp))
            self._num_unresolved += 1
            return handle

    def complete(self, handle: ExternalTaskHandle) -> None:
        """The external task received its input."""
        with self._lock:
            self._completed.append(handle)
            self._num_unresolved -= 1

    def report_lost_input(
        self, handle: ExternalTaskHandle, error: ObjectLostError
    ) -> None:
        """The external task's input was lost before it was received. The
        executor re-produces it and hands it out again, possibly to a different
        split.

        Only for a lost input. Abandon a task that failed for any other reason,
        since reconstruction can't help it.
        """
        with self._lock:
            self._lost_inputs.append((handle, error))
            self._num_unresolved -= 1

    def abandon(self, handle: ExternalTaskHandle) -> None:
        """The external task ended without its input for a reason other than
        data loss, e.g. its consumer died. The input isn't re-produced."""
        with self._lock:
            self._abandoned.append(handle)
            self._num_unresolved -= 1

    def finish_split(self, output_split_idx: int) -> None:
        """The consumer of ``output_split_idx`` stopped. Its queued outputs no
        longer keep the executor running.

        A re-produced block that the output splitter routes to a finished split
        is dropped. That's acceptable, since a split that stops early already
        makes the epoch partial.
        """
        with self._lock:
            self._finished_splits.add(output_split_idx)
            # Its consumer stopped, so the outputs it took but never submitted a
            # task for won't be consumed.
            self._outputs_awaiting_submission = {
                block_ref: split_idx
                for block_ref, split_idx in self._outputs_awaiting_submission.items()
                if split_idx != output_split_idx
            }

    def finished_splits(self) -> Set[int]:
        with self._lock:
            return set(self._finished_splits)

    def has_unresolved(self) -> bool:
        """Whether any taken output or submitted task isn't resolved yet,
        including reports the executor hasn't registered."""
        with self._lock:
            return bool(
                self._outputs_awaiting_submission
                or self._num_unresolved
                or self._completed
                or self._lost_inputs
                or self._abandoned
            )

    def update_lineage(self, lineage_tracker: Optional["LineageTracker"]) -> None:
        """Update the lineage external consumer task status since the last call.
        Executor thread only.

        Registers submitted tasks and completes completed ones with
        ``lineage_tracker``, if there is one, which garbage collects them. Drops
        abandoned ones without telling the tracker. Sets the
        lost inputs reported with them aside for ``pop_lost_inputs``. Their
        tasks are registered by then, since a task is always submitted before
        its input can be reported lost.

        Args:
            lineage_tracker: The executor's lineage tracker, or None if lineage
                reconstruction is disabled.
        """
        with self._lock:
            submitted, self._submitted = self._submitted, []
            completed, self._completed = self._completed, []
            lost_inputs, self._lost_inputs = self._lost_inputs, []
            abandoned, self._abandoned = self._abandoned, []

        for handle, block_ref, stamp in submitted:
            if stamp is not None:
                lineage_task_id = stamp.lineage_task_id
                plan_ids = stamp.reconstruction_plan_ids
            else:
                lineage_task_id, plan_ids = f"consumer:{handle}", frozenset()
            self._lineage_ids[handle] = (lineage_task_id, plan_ids)
            if lineage_tracker is not None:
                dependencies = lineage_tracker.resolve_dependencies([block_ref.hex()])
                if plan_ids:
                    for plan_id in sorted(plan_ids):
                        lineage_tracker.register_task_submission(
                            lineage_task_id, dependencies, plan_id
                        )
                else:
                    lineage_tracker.register_task_submission(
                        lineage_task_id, dependencies
                    )

        for handle in completed:
            lineage_task_id, plan_ids = self._lineage_ids.pop(handle)
            if lineage_tracker is None:
                continue
            # The task produces nothing, so completing it garbage collects it
            # and whatever lineage only it still needed.
            if plan_ids:
                for plan_id in sorted(plan_ids):
                    lineage_tracker.register_task_complete(
                        lineage_task_id, num_outputs=0, reconstruction_plan_id=plan_id
                    )
            else:
                lineage_tracker.register_task_complete(lineage_task_id, num_outputs=0)

        for handle in abandoned:
            del self._lineage_ids[handle]

        self._lost_inputs_to_pop.extend(
            ExternalTaskLostInput(*self._lineage_ids.pop(handle), error)
            for handle, error in lost_inputs
        )

    def pop_lost_inputs(self) -> List[ExternalTaskLostInput]:
        """Return the lost inputs set aside by ``update_lineage``, and forget
        them. Executor thread only.

        A loss reported after the last ``update_lineage`` isn't returned until
        the next one registers its task.

        Returns:
            The tasks whose input was lost, for the executor to reconstruct.
        """
        lost_inputs, self._lost_inputs_to_pop = self._lost_inputs_to_pop, []
        return lost_inputs
