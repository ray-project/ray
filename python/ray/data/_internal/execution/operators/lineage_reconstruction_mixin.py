import abc
import logging
from dataclasses import replace
from typing import Any, Dict, FrozenSet, Iterable, Iterator, List, Optional, Set, Tuple

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
        # Mapping of reconstruction plan ID -> blocks withheld (expressed as a mapping of parent block output to actual RefBundle) until every parent of a child reconstruction
        # task has produced the required blocks, and the child reconstruction task(s) can be scheduled with
        # the complete set(s) of input blocks. For each reconstruction plan ID, whichever parent task produces its last
        # withheld output last hands over the input set for that plan as one bundle carrying the child reconstruction task's `ReconstructionStamp`.
        # See `_release_reconstruction_blocks_to_child` for more details.
        self._reconstruction_outputs: Dict[
            ReconstructionPlanId, Dict[ParentBlockOutput, RefBundle]
        ] = {}
        # Mapping of lineage task ID -> the plans batched into its reconstruction task
        # while it is queued but not yet submitted. An entry in this dict exists exactly for the duration the task
        # (in the dict key) is queued. Batched plans are added to the value set the task while it is queued and added to
        # the plans the task serves on submission.
        # A plan that traces back to a queued seed is batched here instead of
        # re-injecting the seed.
        self._batched_plans: Dict[LineageTaskId, Set[ReconstructionPlanId]] = {}
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
    ) -> Tuple[LineageTaskId, FrozenSet[ReconstructionPlanId], List[ParentBlockOutput]]:
        """Return the metadata this task that is being submitted should register with ``lineage_tracker``.

        Only called for a lineage-tracked operator. Returns
        ``(lineage_task_id, reconstruction_plan_ids, dependencies)``, where ``reconstruction_plan_ids`` are the
        reconstruction plans this task serves if it is a reconstruction task, and empty for a fresh task.
        A reconstruction task serves the plans on its stamp, plus any plans batched into it
        while it was queued.

        A fresh task returns lineage task ID ``f"{operator_id}:{task_index}"``. A *reconstruction* attempt
        must re-use the original logical id of the original task attempt, or the plan never resolves and
        the graph grows a duplicate node.
        """
        # Pass every ref rather than just ``block_refs[0]``. `RebundleQueue` parks
        # zero-row bundles and prepends them on the next merge, so the block of
        # interest is not necessarily first.
        dependencies = lineage_tracker.resolve_dependencies(
            block_ref.hex()  # pyrefly: ignore[missing-attribute]
            for block_ref in inputs.block_refs
        )

        stamp = inputs.reconstruction_stamp
        if stamp is not None:
            assert self.owns_data_task(stamp.lineage_task_id), stamp
            # Pop the plans batched in while this task attempt was queued
            batched_plan_ids = self._batched_plans.pop(stamp.lineage_task_id, None)
            if batched_plan_ids is None:
                raise ValueError(
                    f"Task {stamp.lineage_task_id} was submitted as a "
                    "reconstruction task, but no reconstruction task for it was queued."
                )
            reconstruction_plan_ids = stamp.reconstruction_plan_ids
            if batched_plan_ids:
                # Add the batched plans to the plans on the reconstruction stamp
                reconstruction_plan_ids = reconstruction_plan_ids | batched_plan_ids
            return stamp.lineage_task_id, reconstruction_plan_ids, dependencies

        return self._lineage_task_id_for(task_index), frozenset(), dependencies

    def stamp_seed_reconstruction_task(
        self,
        seed_task_id: LineageTaskId,
        reconstruction_plan_ids: Iterable[ReconstructionPlanId],
        seed_input: RefBundle,
    ) -> RefBundle:
        """Prepare a seed task's retained input to be re-injected for reconstruction.

        Stamping marks the input as a reconstruction task for ``seed_task_id`` rather than a
        fresh task. When the seed operator submits it, the task registers under the
        seed's original lineage task ID (rather than creating a new task node/ID),
        and we classify the outputs of the task (whether pruned/dropped/kept) per reconstruction plan.

        While a seed task's input bundle is queued, any later reconsturction plans due to object losses that trace back to
        the same seed task are batched with the current queued attempt instead of re-injecting the seed again.
        On submission the task serves the plans on the stamp and the batched plans, and the stamps it releases to its child reconstruction tasks carry all of them.

        Args:
            seed_task_id: The ID of the seed task to reconstruct.
            reconstruction_plan_ids: The reconstruction plans the reconstruction task serves.
            seed_input: The retained input of the seed task.

        Returns:
            The seed input, stamped for re-injection.
        """
        plan_ids = frozenset(reconstruction_plan_ids)
        assert plan_ids, seed_task_id
        self._queue_reconstruction_task(seed_task_id)
        return replace(
            seed_input,
            reconstruction_stamp=ReconstructionStamp(
                lineage_task_id=seed_task_id,
                reconstruction_plan_ids=plan_ids,
            ),
        )

    def _queue_reconstruction_task(self, lineage_task_id: LineageTaskId) -> None:
        """Record that a stamped input for ``lineage_task_id`` was queued, so plans can
        be batched into that reconstruction task until it is submitted."""
        assert lineage_task_id not in self._batched_plans, lineage_task_id
        # The stamp holds the plans the input was queued with. Plans batched in later
        # go to `_batched_plans` until submission.
        self._batched_plans[lineage_task_id] = set()

    def join_queued_reconstruction_task(
        self,
        lineage_task_id: LineageTaskId,
        reconstruction_plan_ids: Iterable[ReconstructionPlanId],
    ) -> bool:
        """Batch plans into a reconstruction task of this operator that is
        queued but not yet submitted (its input bundle is still in an operator queue).

        Args:
            lineage_task_id: The lineage task ID of the reconstruction task.
            reconstruction_plan_ids: The reconstruction plans to add.

        Returns:
            True if the queued reconstruction task now serves the plans. False if no
            reconstruction task for it is queued, so the caller must queue one.
        """
        batched_plan_ids = self._batched_plans.get(lineage_task_id)
        if batched_plan_ids is None:
            return False
        batched_plan_ids.update(reconstruction_plan_ids)
        return True

    def _last_withheld_output_index(
        self,
        lineage_task_id: LineageTaskId,
        reconstruction_plan_ids: Iterable[ReconstructionPlanId],
    ) -> Optional[int]:
        """The highest output index block that is withheld by a task for its child reconstruction tasks across all
        of the reconstruction plans it serves.

        Withheld outputs are the ones the lineage tracker reports as ``OBJECT_REUSED``.
        Called when the task is submitted. Nothing is released yet then, so every child
        reconstruction task the task withholds outputs for is still pending.

        Args:
            lineage_task_id: The lineage task ID of the task.
            reconstruction_plan_ids: The reconstruction plans the task serves.

        Returns:
            The highest withheld output index, or ``None`` if the task withholds no
            outputs for the plans.
        """
        lineage_tracker = self._lineage_tracker
        assert lineage_tracker is not None
        return max(
            (
                parent_block_dependency.output_index
                for reconstruction_plan_id in reconstruction_plan_ids
                for parent_block_dependencies in lineage_tracker.get_pending_children(
                    lineage_task_id, reconstruction_plan_id
                ).values()
                for parent_block_dependency in parent_block_dependencies
                if parent_block_dependency.parent_lineage_task_id == lineage_task_id
            ),
            default=None,
        )

    def _consumer_of(
        self, child_task_id: LineageTaskId
    ) -> "LineageReconstructionMixin":
        """The downstream operator that owns the child reconstruction task ``child_task_id``."""
        for op in self.output_dependencies:
            if isinstance(op, LineageReconstructionMixin) and op.owns_data_task(
                child_task_id
            ):
                return op
        raise ValueError(
            f"No downstream operator of {self.name} owns child reconstruction task {child_task_id}."
        )

    def _release_reconstruction_blocks_to_child(
        self,
        lineage_task_id: str,
        reconstruction_plan_ids: FrozenSet[ReconstructionPlanId],
        task_index: int,
    ) -> None:
        """Release the reconstruction blocks that are now ready to be consumed by a child reconstruction task,
        for the reconstruction plans the parent task serves. Called when the parent produces its last withheld output,
        and again from the parent's task completed callback as a fallback.

        We 'release' blocks when the required block inputs for a downstream child reconstruction task are now ready to be consumed.
        We do this in the following steps for each pending child reconstruction task:
        1. Find the plans whose withheld blocks hold the child reconstruction task's whole input set
        2. Pop the child reconstruction task's blocks from the withheld outputs of each of those plans
        3. Merge the blocks into a single RefBundle, stamped with the child reconstruction task and all of those plans
        4. Add the bundle to the operator's output queue, so the child reconstruction task can be scheduled.

        A child reconstruction task is scheduled, and consumes the bundle from the operator's output queue. On discovering that it is a
        reconstruction task (from the stamp attribute on the bundle), the child reconstruction task knows the correct task ID to register
        with and which reconstruction plans it serves.

        For a downstream child reconstruction task, the parent task that produces its last withheld output last for a plan
        holds the full input set of reconstruction blocks for that plan. Plans whose input sets complete at the same time
        share one release, so the child reconstruction task runs once for all of them. A plan that is still missing blocks keeps them withheld,
        and is released when its last parent produces its last withheld output.

        Args:
            lineage_task_id: The lineage task ID of the parent task.
            reconstruction_plan_ids: The set of reconstruction plans served by the parent task.
                This includes the plans on the reconstruction stamp on the task's input bundle and any plans batched into it while the task was queued.
            task_index: The index of the parent task. Released bundles are added to
                the output queue under this key, so they are ordered with that task's outputs.
        """
        lineage_tracker = self._lineage_tracker
        assert lineage_tracker is not None
        # Mapping of pending child reconstruction task ID -> its parent block dependencies, and the plans waiting on it.
        # A child reconstruction task's parent block dependencies come from its first attempt, so they are the same under every plan.
        child_to_parent_block_dependencies: Dict[
            LineageTaskId, List[ParentBlockOutput]
        ] = {}
        child_to_plans: Dict[LineageTaskId, List[ReconstructionPlanId]] = {}
        for reconstruction_plan_id in sorted(reconstruction_plan_ids):
            for (
                child_task_id,
                parent_block_dependencies,
            ) in lineage_tracker.get_pending_children(
                lineage_task_id, reconstruction_plan_id
            ).items():
                child_to_parent_block_dependencies[
                    child_task_id
                ] = parent_block_dependencies
                child_to_plans.setdefault(child_task_id, []).append(
                    reconstruction_plan_id
                )

        # Each child reconstruction task's blocks come back in its first attempt's input order, across all
        # of its parents. Re-running the child reconstruction task on that exact order keeps every output
        # index holding the rows it held originally, which output reuse relies on.
        for child_task_id in sorted(child_to_parent_block_dependencies):
            parent_block_dependencies = child_to_parent_block_dependencies[
                child_task_id
            ]
            ready_plan_ids = []
            for reconstruction_plan_id in child_to_plans[child_task_id]:
                held_blocks = self._reconstruction_outputs.get(
                    reconstruction_plan_id, {}
                )
                missing = [
                    parent_block_dependency
                    for parent_block_dependency in parent_block_dependencies
                    if parent_block_dependency not in held_blocks
                ]
                if missing:
                    # Some parent of this child reconstruction task has not re-produced its share for this plan yet. Wait.
                    # Every parent serving the plan runs this at its last withheld output, so
                    # whichever parent reaches it last holds the whole set. Submitting now would
                    # run the child reconstruction task against part of its input and silently emit a subset of its rows.
                    logger.debug(
                        "[lineage-reconstruction] Child reconstruction task %s of plan %s is not ready: %d of "
                        "its input blocks are still pending (missing %s, held %s).",
                        child_task_id,
                        reconstruction_plan_id,
                        len(missing),
                        missing,
                        sorted(
                            held_blocks,
                            key=lambda parent_block_dependency: (
                                parent_block_dependency.parent_lineage_task_id,
                                parent_block_dependency.output_index,
                            ),
                        ),
                    )
                    continue
                ready_plan_ids.append(reconstruction_plan_id)
            if not ready_plan_ids:
                continue

            # Pop the child reconstruction task's blocks from every ready plan. Plans that shared a parent's
            # reconstruction task hold the same blocks. Otherwise each plan holds its own copy of
            # the same rows, so the first plan's blocks serve all of them.
            blocks_per_plan = [
                [
                    self._reconstruction_outputs[plan_id].pop(parent_block_dependency)
                    for parent_block_dependency in parent_block_dependencies
                ]
                for plan_id in ready_plan_ids
            ]
            # Register with the lineage tracker that all the blocks for this task are now
            # queued for each of the plans
            for reconstruction_plan_id in ready_plan_ids:
                for parent_block_dependency in parent_block_dependencies:
                    lineage_tracker.register_block_queued(
                        parent_block_dependency.parent_lineage_task_id,
                        parent_block_dependency.output_index,
                        reconstruction_plan_id,
                    )
                if not self._reconstruction_outputs[reconstruction_plan_id]:
                    del self._reconstruction_outputs[reconstruction_plan_id]
            # If the child reconstruction task is already queued, batch these plans into
            # it instead of queueing it again. The queued input holds the same
            # rows, so these blocks are dropped.
            consumer = self._consumer_of(child_task_id)
            if consumer.join_queued_reconstruction_task(child_task_id, ready_plan_ids):
                continue
            # Create a complete RefBundle containing all reconstruction blocks withheld for a child reconstruction task,
            # stamp it with the child reconstruction task's lineage task ID and every plan it serves.
            inputs = replace(
                RefBundle.merge_ref_bundles(blocks_per_plan[0]),
                reconstruction_stamp=ReconstructionStamp(
                    lineage_task_id=child_task_id,
                    reconstruction_plan_ids=frozenset(ready_plan_ids),
                ),
            )
            consumer._queue_reconstruction_task(child_task_id)
            # Add to the output queue of the current task
            self._enqueue_reconstruction_output(inputs, task_index)

    def _iter_reconstruction_outputs(self) -> Iterator[RefBundle]:
        """The re-produced outputs withheld for child reconstruction tasks not yet ready."""
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
        self._batched_plans.clear()
