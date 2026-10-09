import abc
import logging
from dataclasses import replace
from typing import Any, Dict, Iterator, List, Optional, Tuple

from typing_extensions import override

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import ReconstructionStamp
from ray.data._internal.execution.lineage_tracker import (
    LineageTaskId,
    LineageTracker,
    ParentBlockOutput,
    ReconstructionPlanId,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)

logger = logging.getLogger(__name__)


class LineageReconstructionMixin(InternalQueueOperatorMixin, abc.ABC):
    """An operator that supports lineage reconstruction.

    This means that the operator's tasks are recorded in the lineage graph, and if
    the output of a downstream task (that also supports lineage reconstruction) is lost,
    the operator can reconstruct it by re-executing the tasks in the chain of dependencies.

    This extends ``InternalQueueOperatorMixin`` because during lineage reconstruction, the operator
    has to withhold reconstructed outputs until a downstream task's whole set of input blocks is ready.
    Those withheld outputs are stored in an internal output buffer, so the operator isn't considered complete
    until the outputs are released to the downstream task.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        # For lineage reconstruction:
        # Mapping of reconstruction plan ID -> blocks withheld (expressed as a mapping of parent block output to actual RefBundle) until every parent of a reconstruction
        # child has produced the required blocks, and the reconstruction child task(s) can be scheduled with
        # the complete set(s) of input blocks. For each reconstruction plan ID, whichever parent task finishes last hands
        # over the input set for that plan as one bundle carrying the child's `ReconstructionStamp`.
        # See `_release_reconstruction_blocks_to_child` for more details.
        self._reconstruction_outputs: Dict[
            ReconstructionPlanId, Dict[ParentBlockOutput, RefBundle]
        ] = {}
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def _enqueue_reconstruction_output(self, bundle: RefBundle, task_index: int):
        """Add a released reconstruction input bundle to this operator's output
        queue, on behalf of the task with ``task_index``."""
        ...

    def _is_seed_operator(self) -> bool:
        """Whether this op is a seed operator for lineage reconstruction.

        It is one when it consumes directly from an ``InputDataBuffer`` -- a
        source with no upstream lineage, whose output bundle is therefore the
        durable, resubmittable seed input.
          - ``Read`` (V1 / ``range``, input is a ``ReadTask``) and ``ReadFiles``
            (V2, input is a ``FileManifest``): the input is tiny metadata and
            re-running the task reproduces the data.
          - ``from_blocks`` / ``from_items`` / ``from_pandas`` / ``from_arrow``
            and cached blocks: the input *is* the data, captured durably as-is.
        """
        from ray.data._internal.execution.operators.input_data_buffer import (
            InputDataBuffer,
        )

        return all(
            isinstance(input_op, InputDataBuffer)
            for input_op in self.input_dependencies
        )

    def _lineage_task_id_for(self, task_index: int) -> str:
        """The lineage id of this operator's ``task_index``-th fresh task. This provides a logical ID (stable across task reconstruction attempts) for lineage tracking."""
        return f"{self.id}:{task_index}"

    def owns_data_task(self, lineage_task_id: str) -> bool:
        """Whether this operator owns the data task with the given lineage task ID."""
        return lineage_task_id.rsplit(":", 1)[0] == self.id

    def _lineage_for_submission(
        self,
        lineage_tracker: LineageTracker,
        task_index: int,
        inputs: RefBundle,
    ) -> Tuple[LineageTaskId, Optional[ReconstructionPlanId], List[ParentBlockOutput]]:
        """Return the metadata this task that is being submitted should register with ``lineage_tracker``.

        Only called for a lineage-tracked operator. Returns
        ``(lineage_task_id, reconstruction_plan_id, dependencies)``, where ``reconstruction_plan_id`` is the reconstruction plan ID
        if this is a reconstruction task. ``reconstruction_plan_id`` is ``None`` for a fresh task.

        A fresh task returns lineage task ID ``f"{operator_id}:{task_index}"``. A *reconstruction* attempt
        must re-use the original logical id of the original task attempt, or the plan never resolves and
        the graph grows a duplicate node.
        """
        # Pass every ref rather than just ``block_refs[0]``. `RebundleQueue` parks
        # zero-row bundles and prepends them on the next merge, so the block of
        # interest is not necessarily first.
        dependencies = lineage_tracker.resolve_dependencies(
            block_ref.hex() for block_ref in inputs.block_refs
        )

        stamp = inputs.reconstruction_stamp
        if stamp is not None:
            assert self.owns_data_task(stamp.lineage_task_id), stamp
            return stamp.lineage_task_id, stamp.reconstruction_plan_id, dependencies

        return self._lineage_task_id_for(task_index), None, dependencies

    def _release_reconstruction_blocks_to_child(
        self, lineage_task_id: str, reconstruction_plan_id: str, task_index: int
    ) -> None:
        """Release the reconstruction blocks that are now ready to be consumed by a child reconstruction task,
        as part of a reconstruction plan. Called from the completing parent's task completed callback.

        We 'release' blocks when the required block inputs for a downstream child task as part of the given reconstruction_plan_id
        are now ready to be consumed. We do this in the following steps:
        1. Pop the reconstruction outputs withheld for a particular reconstruction_plan_id
        2. Merge the popped blocks into a single RefBundle, setting the 'reconstruction stamp' attribute of this bundle
        3. Add the bundle to the operator's output queue, so the child task can be scheduled.

        A child task is scheduled, and consumes the bundle from the operator's output queue. On discovering that it is a
        reconstruction task (from the stamp attribute on the bundle), the child task knows the correct task ID to register
        with and which reconstruction plan it belongs to.

        For a downstream reconstruction child task, the parent task that completes last holds the full input
        set of reconstruction blocks, and can release it to the operator's output queue.
        """
        held_blocks = self._reconstruction_outputs.get(reconstruction_plan_id, {})
        # Each child's blocks come back in its first attempt's input order, across all
        # of its parents. Re-running the child on that exact order keeps every output
        # index holding the rows it held originally, which output reuse relies on.
        for child_task_id, slots in self._lineage_tracker.get_pending_children(
            lineage_task_id, reconstruction_plan_id
        ).items():
            missing = [slot for slot in slots if slot not in held_blocks]
            if missing:
                # Some parent of this child has not re-produced its share yet. Wait:
                # every parent serving the plan runs this on completion, and a parent's
                # outputs are withheld before its own done-callback, so whichever
                # completes last holds the whole set. Submitting now would run the child
                # against part of its input and silently emit a subset of its rows.
                logger.debug(
                    "[lineage-reconstruction] Child %s of plan %s is not ready: %d of "
                    "its input blocks are still pending (missing %s, held %s).",
                    child_task_id,
                    reconstruction_plan_id,
                    len(missing),
                    missing,
                    sorted(
                        held_blocks,
                        key=lambda slot: (
                            slot.parent_lineage_task_id,
                            slot.output_index,
                        ),
                    ),
                )
                continue

            # Create a complete RefBundle containing all reconstruction blocks wittheld for a child task,
            # stamp it with the child's lineage task ID and the current reconstruction plan ID.
            inputs = replace(
                RefBundle.merge_ref_bundles([held_blocks.pop(slot) for slot in slots]),
                reconstruction_stamp=ReconstructionStamp(
                    lineage_task_id=child_task_id,
                    reconstruction_plan_id=reconstruction_plan_id,
                ),
            )
            # Register with the lineage tracker that all the blocks for this task in a
            # particular reconstruction plan are now queued
            for slot in slots:
                self._lineage_tracker.register_block_queued(
                    slot.parent_lineage_task_id,
                    slot.output_index,
                    reconstruction_plan_id,
                )
            # Add to the output queue of the current task
            self._enqueue_reconstruction_output(inputs, task_index)

        if not held_blocks:
            self._reconstruction_outputs.pop(reconstruction_plan_id, None)

    def _iter_reconstruction_outputs(self) -> Iterator[RefBundle]:
        """The re-produced outputs withheld for reconstruction children not yet ready."""
        for held in self._reconstruction_outputs.values():
            yield from held.values()

    @override
    def internal_output_queue_num_blocks(self) -> int:
        # Withheld reconstruction outputs are outputs this operator still owes a
        # consumer, so they have to count towards the operator's total number of blocks.
        return super().internal_output_queue_num_blocks() + sum(
            len(bundle.blocks) for bundle in self._iter_reconstruction_outputs()
        )

    @override
    def clear_internal_output_queue(self) -> None:
        super().clear_internal_output_queue()
        self._reconstruction_outputs.clear()
