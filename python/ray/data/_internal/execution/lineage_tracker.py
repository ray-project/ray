import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set, Tuple

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle

logger = logging.getLogger(__name__)

# The ID of a data task. This represents the ID of the initial task execution:
# all retries of that task share the same lineage task ID even if the Ray Core task
# ID differs.
LineageTaskId = str

# The index of one of the output blocks produced by a task.
OutputIndex = int

# The ID of a reconstruction plan. A reconstruction plan ID is represented
# by fresh data task execution it is trying to recover.
ReconstructionPlanId = LineageTaskId

# An opaque, unique ID for one output block. Kept a plain string so this module
# stays free of Ray types.
BlockId = str


class ObjectReuseStatus(Enum):
    """How an output block should be treated during a reconstruction attempt.

    Attributes:
        OBJECT_PRUNED: The object should be ignored and garbage collected.
        OBJECT_NEW: The object is unseen and should be submitted to a fresh task.
        OBJECT_REUSED: The object should be resubmitted for the reconstruction
            attempt.
        OBJECT_UNRELATED: The object is unrelated to this reconstruction attempt.
    """

    OBJECT_PRUNED = 1
    OBJECT_NEW = 2
    OBJECT_REUSED = 3
    OBJECT_UNRELATED = 4


@dataclass(frozen=True)
class ChildBlockDependency:
    """A tuple to associate a child task to the
    output index of the parent that the child task depends on.
    """

    child_lineage_task_id: LineageTaskId
    output_index: OutputIndex


@dataclass(frozen=True)
class ParentBlockOutput:
    """A tuple to associate a parent task to
    one of its outputs indices.
    """

    parent_lineage_task_id: LineageTaskId
    output_index: OutputIndex


@dataclass(eq=False, repr=False)
class TaskNode:
    """A node within the lineage graph tracking the children and parents of a task.

    Note:
        The lineage task ID represents the ID of the initial task execution. All
        retries of the initial task should be represented by the same data task
        ID even if the Ray Core task ID differs.
    """

    lineage_task_id: LineageTaskId
    parent_tasks: List["TaskNode"]
    child_tasks: List["TaskNode"]
    # Maps a child lineage task ID to the indices of the outputs produced by this
    # task that the child task depends on.
    child_task_block_dependencies: Dict[LineageTaskId, List[OutputIndex]]

    # Reconstruction plans that are currently in flight on this task.
    # Maps a reconstruction plan ID to the child block dependencies the plan must re-produce.
    plan_to_child_block_lineages: Dict[ReconstructionPlanId, Set[ChildBlockDependency]]

    # The blocks this task consumes (tuples of parent task ID and output index), in order as they were
    # consumed during the task's first attempt. A reconstruction re-runs the task on exactly this order,
    ordered_parent_block_dependencies: List[ParentBlockOutput] = field(
        default_factory=list
    )

    # How many outputs this task added to this operators output queue.
    # This is only incremented by fresh tasks and tasks that are the reconstruction plan target,
    # which are the only tasks that explicitly add outputs to the operator queue
    num_queued_outputs: int = 0

    # The input of a seed task (a task with no parent tasks), kept so lineage
    # reconstruction can resubmit it. ``None`` for every non-seed task.
    seed_input: Optional["RefBundle"] = None

    # --- Garbage collection state (see ``LineageTracker._collect``). ---

    # Whether this task's outputs are durable, so nothing downstream ever needs
    # them reconstructed.
    is_durable: bool = False
    # Number of outputs the task produced, known once an attempt completes. None
    # until then, so a task node that never completed is never garbage collected.
    num_outputs: Optional[int] = None
    # Attempts submitted and not yet completed or failed, counted once per
    # reconstruction plan the attempt serves. A node is never collected while an
    # attempt is running, since the attempt still needs the task node metadata to track the reconstruction.
    num_running_attempts: int = 0
    # Indices of this task's outputs whose consumer has been collected. The
    # consumer is gone from ``child_tasks``, but its rows are durable in a sink.
    durable_output_indices: Set[OutputIndex] = field(default_factory=set)
    # IDs of this task's output blocks that have an entry in the tracker's block
    # map. The node owns those entries: resolving a block removes its entry, and
    # removing the node removes the rest (see ``LineageTracker._remove_task_node``).
    output_block_ids: Set[BlockId] = field(default_factory=set)

    def __repr__(self) -> str:
        parent_ids = [task.lineage_task_id for task in self.parent_tasks]
        child_ids = [task.lineage_task_id for task in self.child_tasks]
        return (
            f"{type(self).__name__}(lineage_task_id={self.lineage_task_id!r}, "
            f"parent_tasks={parent_ids!r}, child_tasks={child_ids!r}, "
            f"child_task_block_dependencies={self.child_task_block_dependencies!r}, "
            f"plan_to_child_block_lineages={self.plan_to_child_block_lineages!r}, "
            f"num_queued_outputs={self.num_queued_outputs!r}, "
            f"is_durable={self.is_durable!r}, num_outputs={self.num_outputs!r}, "
            f"num_running_attempts={self.num_running_attempts!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskNode):
            return False
        return self.lineage_task_id == other.lineage_task_id

    def __hash__(self) -> int:
        return hash(self.lineage_task_id)


class LineageTracker:
    def __init__(self):
        self._lineage_task_id_to_task_node: Dict[LineageTaskId, TaskNode] = {}
        # Mapping of block ID -> the parent task output (task ID, output index) that produced it.
        self._block_id_to_parent_output: Dict[BlockId, ParentBlockOutput] = {}
        # Lost objects Ray Data tried to rebuild, counted like Ray Core's
        # ``object_recovery_attempts{Type="Reconstruct"}``.
        self.num_object_reconstruction_attempts = 0

    def register_block_output(
        self,
        lineage_task_id: LineageTaskId,
        block_id: BlockId,
        output_index: OutputIndex,
    ) -> None:
        """Record which task produced a block, so that whichever task later
        consumes it can fetch the dependency easily with the block ID.

        The entry belongs to the producing task's node. It is removed when a task
        consuming the block resolves it, or else with the node.

        Args:
            lineage_task_id: The ID of the data task that produced the block.
            block_id: Opaque ID of the produced block, unique per block.
            output_index: The index of this output within the producing task.

        Raises:
            ValueError: If the task is not registered.
                Invariant: a task is registered before it produces outputs.
        """
        task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before "
                "recording its output but was not."
            )
        task_node.output_block_ids.add(block_id)
        self._block_id_to_parent_output[block_id] = ParentBlockOutput(
            parent_lineage_task_id=lineage_task_id, output_index=output_index
        )

    def register_block_queued(
        self,
        lineage_task_id: LineageTaskId,
        output_index: OutputIndex,
        reconstruction_plan_id: Optional[ReconstructionPlanId] = None,
    ) -> None:
        """Unlike ``register_block_output``, which records every block a task
        produces, this is only called for a block the operator adds to the operator's output queue.
        A block that is withheld or dropped is not queued.

        A queued block counts as delivered even if no task has consumed it yet.
        If a queued block is lost, the task that consumes it fails, and that failure
        opens its own reconstruction plan.

        Args:
            lineage_task_id: The ID of the data task that produced the block.
            output_index: The index of this output within the producing task.
            reconstruction_plan_id: The reconstruction plan ID of the attempt
                that queued the block. ``None`` for a fresh attempt.
        """
        task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before "
                "queueing its output but was not."
            )

        # Fresh attempt, or a reconstruction task where this is the target/leaf node of the reconstruction plan.
        # Both queue outputs in index order, so a count is enough to record which outputs are downstream.
        if reconstruction_plan_id is None or reconstruction_plan_id == lineage_task_id:
            if output_index != task_node.num_queued_outputs:
                raise ValueError(
                    f"Task {lineage_task_id} queued output {output_index}, but "
                    f"{task_node.num_queued_outputs} outputs were already queued. "
                    "Outputs must be queued once each, in index order."
                )
            task_node.num_queued_outputs += 1
            return

        # Each block has one consumer, so at most one child task matches this output index.
        block_index_child_task_dependencies = (
            task_node.plan_to_child_block_lineages.get(reconstruction_plan_id, set())
        )
        entry = next(
            (
                owed_block
                for owed_block in block_index_child_task_dependencies
                if owed_block.output_index == output_index
            ),
            None,
        )
        if entry is None:
            raise ValueError(
                f"Task {lineage_task_id} queued output {output_index} for plan "
                f"{reconstruction_plan_id}, but the plan does not owe it. Was it "
                "already queued for this plan?"
            )
        block_index_child_task_dependencies.remove(entry)
        if not block_index_child_task_dependencies:
            del task_node.plan_to_child_block_lineages[reconstruction_plan_id]

    def resolve_dependencies(
        self, block_ids: Iterable[BlockId]
    ) -> List[ParentBlockOutput]:
        """Resolve the (producing task logical ID, output index) pair each of the blocks corresponds to
        for a task that is being submitted.

        Blocks with no recorded producer are skipped: they were not produced by
        a tracked task. A seed task's input, for instance, comes from the source
        rather than from any task, so it will have no producer task.

        Args:
            block_ids: IDs of the blocks the task takes as input.

        Returns:
            The dependencies to pass to ``register_task_submission``.

        Note:
            Entries are consumed since a block is only ever dispatched to one
            task.
        """
        dependencies = []
        for block_id in block_ids:
            parent_output = self._block_id_to_parent_output.pop(block_id, None)
            if parent_output is None:
                continue
            # The producer owns the entry, so it is registered.
            self._lineage_task_id_to_task_node[
                parent_output.parent_lineage_task_id
            ].output_block_ids.remove(block_id)
            dependencies.append(parent_output)
        return dependencies

    def register_task_submission(
        self,
        lineage_task_id: LineageTaskId,
        dependencies: List[ParentBlockOutput],
        reconstruction_plan_id: Optional[ReconstructionPlanId] = None,
    ) -> None:
        """Register a newly submitted task with the lineage graph.

        Repeated registration of the same lineage task ID only records another
        running attempt; the node and its edges are left as they are.

        Args:
            lineage_task_id: The ID of the data task that was submitted.
            dependencies: The blocks that the task depends on, each pairing a
                parent task ID with the output index of that parent the task
                consumes.
            reconstruction_plan_id: The reconstruction plan ID of the reconstruction attempt if the task is a
                re-execution. If not provided, the task is assumed to be a fresh
                attempt.

        Raises:
            ValueError: Raises if a parent task is not registered.
                        Invariant: a task can only be submitted once all
                        of its parents have been submitted.

                        Raises if a previously unseen task is submitted with a ``reconstruction_plan_id``.
                        Invariant: only a task that has already been submitted
                        can be a re-execution of a reconstruction plan.

                        Raises if a ``reconstruction_plan_id`` was provided for the task to register,
                        but a parent of the task still owes one of its input
                        blocks for the plan.
                        Invariant: a re-execution is only submitted on input
                        that its parents queued for the plan
                        (``register_block_queued``).
        """
        logger.debug(
            "Registering task submission for task %s with dependencies %s",
            lineage_task_id,
            dependencies,
        )

        re_executed_task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if reconstruction_plan_id is not None:
            if re_executed_task_node is None:
                raise ValueError(
                    f"Task {lineage_task_id} was submitted as a re-execution "
                    f"of plan {reconstruction_plan_id}. However, the task is not registered "
                    "as part of the execution graph. Either the plan was incorrectly "
                    "attributed to this task, or the task was incorrectly removed "
                    "from the execution graph."
                )
            if (
                reconstruction_plan_id
                not in re_executed_task_node.plan_to_child_block_lineages
            ):
                raise ValueError(
                    f"Task {lineage_task_id} was submitted as a re-execution "
                    f"of plan {reconstruction_plan_id}. However, the plan is not registered "
                    "as a known reconstruction plan for this task. Has the "
                    "plan been correctly added to all tasks necessary for "
                    "the reconstruction?"
                )

        # Construct the child to parent edges. A child may depend on several
        # blocks from the same parent, so dedupe parents while grouping all of
        # that parent's block dependencies together.
        parent_to_dependencies: Dict[LineageTaskId, List[OutputIndex]] = {}
        parent_task_nodes: List[TaskNode] = []
        for dependency in dependencies:
            parent_task_node = self._lineage_task_id_to_task_node.get(
                dependency.parent_lineage_task_id
            )
            if parent_task_node is None:
                raise ValueError(
                    f"Expected parent task {dependency.parent_lineage_task_id} to "
                    f"be registered before child task {lineage_task_id} but was not."
                )
            parent_task_child_block_lineages = (
                parent_task_node.plan_to_child_block_lineages
            )
            # reconstruction task case: a re-execution attempt only runs on inputs its
            # parents already released. Raise if the plan still owes any of it.
            if reconstruction_plan_id is not None:
                dependency_still_owed = ChildBlockDependency(
                    child_lineage_task_id=lineage_task_id,
                    output_index=dependency.output_index,
                )
                if dependency_still_owed in parent_task_child_block_lineages.get(
                    reconstruction_plan_id, set()
                ):
                    raise ValueError(
                        f"Task {lineage_task_id} was submitted for plan "
                        f"{reconstruction_plan_id} with input "
                        f"{dependency_still_owed} from parent "
                        f"{dependency.parent_lineage_task_id}, but that block was "
                        "never queued for the plan. Was the task submitted before "
                        "its parent released its input?"
                    )
            # fresh task case: add dependencies as edge from parent to child.
            else:
                if dependency.parent_lineage_task_id not in parent_to_dependencies:
                    parent_task_nodes.append(parent_task_node)
                    parent_to_dependencies[dependency.parent_lineage_task_id] = []
                parent_to_dependencies[dependency.parent_lineage_task_id].append(
                    dependency.output_index
                )

        # Target task case: the plan no longer claims the target once the target
        # itself is being re-executed.
        if (
            reconstruction_plan_id is not None
            and lineage_task_id == reconstruction_plan_id
        ):
            assert re_executed_task_node is not None
            del re_executed_task_node.plan_to_child_block_lineages[
                reconstruction_plan_id
            ]

        if re_executed_task_node is not None:
            logger.debug(
                "Repeated submission of lineage task ID %s with dependencies %s",
                lineage_task_id,
                dependencies,
            )
            re_executed_task_node.num_running_attempts += 1
            return

        task_node = TaskNode(
            lineage_task_id=lineage_task_id,
            parent_tasks=parent_task_nodes,
            child_tasks=[],
            child_task_block_dependencies={},
            plan_to_child_block_lineages={},
            ordered_parent_block_dependencies=list(dependencies),
            num_running_attempts=1,
        )
        self._lineage_task_id_to_task_node[lineage_task_id] = task_node

        # Construct the parent to child edges.
        for parent_task_node in parent_task_nodes:
            output_indices = parent_to_dependencies[parent_task_node.lineage_task_id]
            parent_task_node.child_tasks.append(task_node)
            block_dependencies = parent_task_node.child_task_block_dependencies
            block_dependencies[lineage_task_id] = output_indices

    def register_task_complete(
        self,
        lineage_task_id: LineageTaskId,
        num_outputs: int,
        reconstruction_plan_id: Optional[ReconstructionPlanId] = None,
    ) -> None:
        """Update the task state for the given data task to completed.

        Called once per reconstruction plan the completed attempt serves. If the
        completion leaves the task collectible (see ``_is_garbage_collectible``), the task
        and any ancestors that thereby become collectible too are garbage
        collected. Completion alone never makes a task's outputs durable; see
        ``try_garbage_collect_lineage``.

        Note:
            Completing a task does not remove anything from its plan set. Plan set entries are
            removed in ``register_block_queued``, when the block is released to
            the child that consumes it.

        Args:
            lineage_task_id: The ID of the data task that was completed.
            num_outputs: How many outputs the attempt produced, so the tracker
                can tell when every output has been consumed.
            reconstruction_plan_id: The reconstruction plan ID of the reconstruction attempt if the task is a
                re-execution. If not provided, the task is assumed to be a fresh
                attempt.

        Raises:
            ValueError: If the task is not already registered.
                Invariant: the task must always be registered before its terminal
                state is reached.
        """
        task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before "
                "completion but was not."
            )
        logger.debug("Registering task complete for task %s", lineage_task_id)
        self._finish_attempt(task_node)
        # A re-execution splits its output the same way as the first attempt,
        # but keep the largest count seen to stay conservative.
        task_node.num_outputs = max(task_node.num_outputs or 0, num_outputs)
        self._collect(task_node)

    def try_garbage_collect_lineage(self, lineage_task_id: LineageTaskId) -> bool:
        """Mark a task's outputs as durable and garbage collect its lineage.

        Call this once a completed task's outputs can never be lost again, for
        example after a write task has written its rows to a sink.

        The task is only collected once it is collectible (see ``_is_garbage_collectible``).
        If it is not yet, for example because another attempt is still running,
        it stays marked as durable and is dropped once that last guard clears.

        Args:
            lineage_task_id: The ID of the data task whose outputs are now durable
                and is ready to try to start garbage collecting from.

        Returns:
            Whether the task was collected by this call.

        Raises:
            ValueError: If the task is not registered.
                Invariant: the task must be registered, and not yet collected,
                when its outputs become durable. A task with no outputs is
                collected at its completion, so call this before completing it.
        """
        task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before "
                "garbage collecting its lineage but was not."
            )
        logger.debug("Marking outputs of task %s as durable", lineage_task_id)
        task_node.is_durable = True
        self._collect(task_node)
        return lineage_task_id not in self._lineage_task_id_to_task_node

    def register_task_failed(
        self,
        lineage_task_id: LineageTaskId,
        reconstruction_plan_id: Optional[ReconstructionPlanId] = None,
    ) -> Tuple[List[LineageTaskId], ReconstructionPlanId]:
        """Mark a task as failed and begin reconstruction of it and its lineage.

        Args:
            lineage_task_id: The ID of the data task that failed.
            reconstruction_plan_id: The reconstruction plan ID of the failed task if the task was a
                re-execution attempt. If not provided, the task is assumed to be
                the target of reconstruction from a fresh attempt.

        Returns:
            A tuple of the lineage task IDs of the seed tasks to resubmit for
            reconstruction, sorted to keep the order deterministic, and the plan
            ID associated with this reconstruction attempt.

        Raises:
            ValueError: If the task, or a child of a task along the failed path,
                is not registered.
                Invariant: the task must always be registered before a terminal
                state is reached.
        """
        task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before getting "
                "a failure status but was not."
            )
        logger.debug(
            "Registering failed task for task %s with reconstruction plan id %s",
            lineage_task_id,
            reconstruction_plan_id,
        )
        self._finish_attempt(task_node)

        seed_task_ids: Set[LineageTaskId] = set()
        # Maps a task's lineage task ID to the IDs of the children it has already
        # been traced from, so that every lineage edge is walked at most once.
        traced_child_ids: Dict[LineageTaskId, Set[LineageTaskId]] = {}

        reconstruction_plan_id_to_attach: ReconstructionPlanId = (
            lineage_task_id
            if reconstruction_plan_id is None
            else reconstruction_plan_id
        )

        def _trace_parent_for_reconstruction(
            node: TaskNode, child_node: Optional[TaskNode] = None
        ) -> None:
            # Sibling failures in the same fan-out trace back to a shared
            # ancestor. That ancestor only needs to re-run once to serve all of
            # its pending children, so an edge that was already traced must not
            # be traced -- and its seed resubmitted -- a second time.
            child_ids = traced_child_ids.setdefault(node.lineage_task_id, set())
            if child_node is not None:
                if child_node.lineage_task_id in child_ids:
                    return
                child_ids.add(child_node.lineage_task_id)

            # Every task along the failed path takes part in the plan. The target
            # of the reconstruction has no child to re-produce blocks for, so its
            # lineage for the plan stays empty.
            child_block_lineages = node.plan_to_child_block_lineages.setdefault(
                reconstruction_plan_id_to_attach, set()
            )

            if child_node is not None:
                if child_node.lineage_task_id not in node.child_task_block_dependencies:
                    raise ValueError(
                        "Failed to construct reconstruction plan for "
                        f"{lineage_task_id}. Expected child task "
                        f"{child_node.lineage_task_id} to be registered before "
                        "getting a failure status but was not."
                    )
                for output_index in node.child_task_block_dependencies[
                    child_node.lineage_task_id
                ]:
                    child_block_lineages.add(
                        ChildBlockDependency(
                            child_lineage_task_id=child_node.lineage_task_id,
                            output_index=output_index,
                        )
                    )
            logger.debug(
                "Task %s has the following plan: %s",
                node.lineage_task_id,
                node.plan_to_child_block_lineages,
            )
            if len(node.parent_tasks) == 0:
                # Idempotent: a seed reached through multiple paths is recorded
                # only once.
                seed_task_ids.add(node.lineage_task_id)
            else:
                for parent_task_node in node.parent_tasks:
                    _trace_parent_for_reconstruction(parent_task_node, node)

        _trace_parent_for_reconstruction(task_node)
        # Sort to keep the order of the seed task IDs deterministic.
        return sorted(seed_task_ids), reconstruction_plan_id_to_attach

    def register_seed_input(
        self, seed_task_id: LineageTaskId, seed_input: "RefBundle"
    ) -> None:
        """Store the input of a seed task, so that lineage reconstruction can resubmit it when trying to reconstruct a lost block.

        Only call this for a seed task's first attempt.

        Args:
            seed_task_id: The ID of the seed task.
            seed_input: The input the seed task was submitted with.

        Raises:
            ValueError: If the task is not registered, or has parent tasks.
                Invariant: The task must be a seed task only (one with no parent tasks).
        """
        task_node = self._lineage_task_id_to_task_node.get(seed_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected seed task {seed_task_id} to be registered before "
                "registering its input but was not."
            )
        if task_node.parent_tasks:
            parent_ids = [task.lineage_task_id for task in task_node.parent_tasks]
            raise ValueError(
                f"Task {seed_task_id} was registered with a seed input but has "
                f"parent tasks {parent_ids}."
            )
        task_node.seed_input = seed_input

    def get_seed_input(self, seed_task_id: LineageTaskId) -> "RefBundle":
        """Get the input retained for the given seed task.

        Args:
            seed_task_id: The ID of a seed task, as returned by
                ``register_task_failed``.

        Returns:
            The input registered with ``register_seed_input``.

        Raises:
            ValueError: If the task is not registered, or is not a seed task with a
                retained input.
        """
        task_node = self._lineage_task_id_to_task_node.get(seed_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected seed task {seed_task_id} to be registered before getting "
                "its input but was not."
            )
        if task_node.seed_input is None:
            raise ValueError(
                f"Task {seed_task_id} has no retained seed input. Either it is not a "
                "seed task, or its input was never registered."
            )
        return task_node.seed_input

    def get_pending_children(
        self,
        lineage_task_id: LineageTaskId,
        reconstruction_plan_id: ReconstructionPlanId,
    ) -> Dict[LineageTaskId, List[ParentBlockOutput]]:
        """Get the children that must be reconstructed for the given data task.

        Children whose input this task already queued for the plan are not
        included.

        Args:
            lineage_task_id: The ID of the data task to get the pending children for.
            reconstruction_plan_id: The ID of the plan to get the pending children for.

        Returns:
            A mapping of each pending child task ID to every block that child
            depends on, across all of its parents, in the child's original input
            order.

        Raises:
            ValueError: If the task is not already registered.
                        Invariant: only registered tasks and their children can \
                        be reconstructed.
        """
        task_node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before getting "
                "pending children but was not."
            )

        child_block_lineages = task_node.plan_to_child_block_lineages.get(
            reconstruction_plan_id, set()
        )
        child_ids_in_plan = {
            child_block_lineage.child_lineage_task_id
            for child_block_lineage in child_block_lineages
        }

        pending_children: Dict[LineageTaskId, List[ParentBlockOutput]] = {}
        for child_task_node in task_node.child_tasks:
            # Only consider children that take an output produced by the plan.
            if child_task_node.lineage_task_id not in child_ids_in_plan:
                continue
            pending_children[child_task_node.lineage_task_id] = list(
                child_task_node.ordered_parent_block_dependencies
            )

        logger.debug(
            "Pending children for task %s with plan: %s -> %s",
            lineage_task_id,
            reconstruction_plan_id,
            child_block_lineages,
        )
        return pending_children

    def get_object_reuse_status(
        self,
        lineage_task_id: LineageTaskId,
        output_index: OutputIndex,
        reconstruction_plan_id: ReconstructionPlanId,
    ) -> ObjectReuseStatus:
        """Get the reuse status of one output block of the given task.

        See :class:`ObjectReuseStatus` for the meaning of each status.

        Args:
            lineage_task_id: The ID of the data task that produced the output.
            output_index: The index of the output object to get the status for.
            reconstruction_plan_id: The ID of the plan to get the status for.

        Returns:
            The reuse status of the output block at ``output_index`` for the
            reconstruction attempt keyed by ``reconstruction_plan_id``.

        Raises:
            ValueError: If the task is not already registered.
        """
        node = self._lineage_task_id_to_task_node.get(lineage_task_id)
        if node is None:
            raise ValueError(
                f"Expected task {lineage_task_id} to be registered before getting "
                "object reuse status but was not."
            )
        task_node: TaskNode = node

        # A plan is keyed by the lineage task ID of the task whose failure opened it.
        is_reconstruction_target = reconstruction_plan_id == lineage_task_id

        def _log_and_return(status: ObjectReuseStatus) -> ObjectReuseStatus:
            logger.debug(
                "Object reuse status for task %s at index %s with plan %s "
                "(target=%s) -> %s gives %s",
                lineage_task_id,
                output_index,
                reconstruction_plan_id,
                is_reconstruction_target,
                task_node.plan_to_child_block_lineages.get(
                    reconstruction_plan_id, set()
                ),
                status.name,
            )
            return status

        if not is_reconstruction_target:
            if reconstruction_plan_id not in task_node.plan_to_child_block_lineages:
                return _log_and_return(ObjectReuseStatus.OBJECT_UNRELATED)
            # For all non-target tasks in the plan, only the outputs that the
            # plan needs should be reconstructed.
            for block_lineage in task_node.plan_to_child_block_lineages[
                reconstruction_plan_id
            ]:
                if block_lineage.output_index == output_index:
                    return _log_and_return(ObjectReuseStatus.OBJECT_REUSED)
            return _log_and_return(ObjectReuseStatus.OBJECT_PRUNED)

        # If the output_index of the block that's being checked for this task is less than the number of
        # outputs queued by an earlier attempt of this task, this block is already downstream,
        # either consumed or waiting in a queue. Queueing it again would duplicate its rows. If
        # that block is lost, the task that consumes it fails and opens its own
        # reconstruction plan separately.

        # TODO(ayushkum): The above doesn't hold for outputs read by an iterator such
        # as iter_batches or take_all. The iterator's ray.get runs outside the
        # executor, so a lost copy never fails a tracked task and no plan opens.
        # A target that loses its own output while running still opens a plan,
        # but the outputs it queued for the iterator earlier aren't re-emitted.
        # Supporting it needs an executor API for consumers to ack or report
        # lost outputs. Address it with the Train + Data FT effort, which needs
        # the same API for streaming_split consumers.
        if output_index < task_node.num_queued_outputs:
            return _log_and_return(ObjectReuseStatus.OBJECT_PRUNED)

        # An output no earlier attempt queued has never been downstream, so the
        # target must queue it.
        return _log_and_return(ObjectReuseStatus.OBJECT_NEW)

    def _finish_attempt(self, task_node: TaskNode) -> None:
        """Mark an attempt running the given task node as finished.

        Args:
            task_node: The task node whose attempt has finished.

        Raises:
            ValueError: If the task node has no running attempt.
                Invariant: every submission reaches exactly one terminal state,
                completed or failed, so an attempt is never finished twice.
        """
        if task_node.num_running_attempts <= 0:
            raise ValueError(
                f"Expected task {task_node.lineage_task_id} to have a running "
                "attempt when finishing one but it had none. Was the attempt "
                "already completed or failed?"
            )
        task_node.num_running_attempts -= 1

    def _is_garbage_collectible(self, task_node: TaskNode) -> bool:
        """Whether nothing can ever need this task re-executed again.

        A node is garbage collectible if the following holds:

        - The task has completed
        - Everything it produced is durable: either its outputs were marked durable
          (``try_garbage_collect_lineage``), or they have each been consumed by a
          task that was itself collected (no more child dependencies).
        - no running attempt, since one would still register outputs and
          completion against this node;
        - no plan set, since one means a reconstruction still expects this task
          to re-run (a seed reconstruction task may be queued for it).

        Args:
            task_node: The task node to check for garbage collectibility.

        Returns:
            Whether the task node is garbage collectible.
        """
        if task_node.num_outputs is None:
            # Never completed.
            return False
        if task_node.num_running_attempts > 0:
            return False
        if task_node.plan_to_child_block_lineages:
            return False
        if task_node.child_tasks:
            return False
        if task_node.is_durable:
            return True
        return all(
            output_index in task_node.durable_output_indices
            for output_index in range(task_node.num_outputs)
        )

    def _collect(self, task_node: TaskNode) -> None:
        """Drop ``task_node`` from the graph if collectible, then its ancestors.

        Args:
            task_node: The task node to collect if collectible.
        """
        pending = [task_node]
        while pending:
            node = pending.pop()
            # A diamond can queue one parent twice; the second visit finds it gone.
            if node.lineage_task_id not in self._lineage_task_id_to_task_node:
                continue
            if not self._is_garbage_collectible(node):
                continue
            self._remove_task_node(node)
            logger.debug("Collected lineage of task %s", node.lineage_task_id)
            pending.extend(node.parent_tasks)
            node.parent_tasks = []

    def _remove_task_node(self, task_node: TaskNode) -> None:
        """Remove ``task_node`` and every piece of state the tracker keeps for it.

        Args:
            task_node: The task node to remove from the graph.
        """
        del self._lineage_task_id_to_task_node[task_node.lineage_task_id]

        for block_id in task_node.output_block_ids:
            del self._block_id_to_parent_output[block_id]
        task_node.output_block_ids.clear()

        for parent_task_node in task_node.parent_tasks:
            parent_task_node.durable_output_indices.update(
                parent_task_node.child_task_block_dependencies.pop(
                    task_node.lineage_task_id
                )
            )
            parent_task_node.child_tasks.remove(task_node)

        task_node.seed_input = None
