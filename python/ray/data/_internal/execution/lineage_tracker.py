import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class ObjectReuseStatus(Enum):
    """
    How an output block should be treated during a reconstruction attempt.

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


@dataclass
class ChildBlockDependency:
    """
    A tuple to associate a child task to the
    output index of the parent that the child task depends on.
    """

    child_data_task_id: str
    output_index: int


@dataclass
class ParentBlockOutput:
    """
    A tuple to associate a parent task to
    one of its outputs indices.
    """

    parent_data_task_id: str
    output_index: int


@dataclass(eq=False, repr=False)
class TaskNode:
    """
    A node within the lineage graph tracking the children and parents of a task.

    Note:
        The data task ID represents the ID of the initial task execution. All
        retries of the initial task should be represented by the same data task
        ID even if the Ray Core task ID differs.
    """

    data_task_id: str
    parent_tasks: List["TaskNode"]
    child_tasks: List["TaskNode"]
    # Maps a child data task ID to the indices of the outputs produced by this
    # task that the child task depends on.
    child_task_block_dependencies: Dict[str, List[int]]

    # Reconstruction plans that are currently in flight on this task.
    # Maps a plan ID to the child block dependencies the plan must re-produce.
    plan_to_child_block_lineages: Dict[str, List[ChildBlockDependency]]

    def __repr__(self) -> str:
        parent_ids = [task.data_task_id for task in self.parent_tasks]
        child_ids = [task.data_task_id for task in self.child_tasks]
        return (
            f"{type(self).__name__}(data_task_id={self.data_task_id!r}, "
            f"parent_tasks={parent_ids!r}, child_tasks={child_ids!r}, "
            f"child_task_block_dependencies={self.child_task_block_dependencies!r}, "
            f"plan_to_child_block_lineages={self.plan_to_child_block_lineages!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskNode):
            return False
        return self.data_task_id == other.data_task_id

    def __hash__(self) -> int:
        return hash(self.data_task_id)


class LineageTracker:
    def __init__(self):
        self._data_task_id_to_task_node: Dict[str, TaskNode] = {}

    def register_task_submission(
        self, data_task_id: str, dependencies: List[ParentBlockOutput]
    ) -> None:
        """
        Register a newly submitted task with the lineage graph.

        Repeated registration of the same data task ID is ignored.

        Args:
            data_task_id: The ID of the data task that was submitted.
            dependencies: The blocks that the task depends on, each pairing a
                parent task ID with the output index of that parent the task
                consumes.

        Raises:
            ValueError: If a parent task is not registered.
                        Invariant: a task can only be submitted once all
                        of its parents have been submitted.
        """
        logger.debug(
            f"Registering task submission for task {data_task_id} with "
            f"dependencies {dependencies}"
        )
        if data_task_id in self._data_task_id_to_task_node:
            logger.debug(
                f"Repeated submission of data task ID {data_task_id} with "
                f"dependencies {dependencies}"
            )
            return

        # Construct the child to parent edges. A child may depend on several
        # blocks from the same parent, so dedupe parents while grouping all of
        # that parent's block dependencies together.
        parent_to_dependencies: Dict[str, List[int]] = {}
        parent_task_nodes: List[TaskNode] = []
        for dependency in dependencies:
            parent_task_node = self._data_task_id_to_task_node.get(
                dependency.parent_data_task_id
            )
            if parent_task_node is None:
                raise ValueError(
                    f"Expected parent task {dependency.parent_data_task_id} to "
                    f"be registered before child task {data_task_id} but was not."
                )
            if dependency.parent_data_task_id not in parent_to_dependencies:
                parent_task_nodes.append(parent_task_node)
                parent_to_dependencies[dependency.parent_data_task_id] = []
            parent_to_dependencies[dependency.parent_data_task_id].append(
                dependency.output_index
            )

        task_node = TaskNode(
            data_task_id=data_task_id,
            parent_tasks=parent_task_nodes,
            child_tasks=[],
            child_task_block_dependencies={},
            plan_to_child_block_lineages={},
        )
        self._data_task_id_to_task_node[data_task_id] = task_node

        # Construct the parent to child edges.
        for parent_task_node in parent_task_nodes:
            output_indices = parent_to_dependencies[parent_task_node.data_task_id]
            parent_task_node.child_tasks.append(task_node)
            block_dependencies = parent_task_node.child_task_block_dependencies
            block_dependencies[data_task_id] = output_indices

    def register_task_complete(
        self, data_task_id: str, plan_id: Optional[str] = None
    ) -> None:
        """
        Update the task state for the given data task to completed.

        Note:
            On re-execution attempts, ``register_task_complete`` must only be
            called after the dependencies of the downstream tasks produced by
            this task are resolved.

        Args:
            data_task_id: The ID of the data task that was completed.
            plan_id: The plan ID of the reconstruction attempt if the task is a
                re-execution. If not provided, the task is assumed to be a fresh
                attempt.

        Raises:
            ValueError: If the task is not already registered, or if ``plan_id``
                is given but no such plan claims the task.
                Invariant: the task must always be registered before a terminal
                state is reached.
        """
        task_node = self._data_task_id_to_task_node.get(data_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before "
                "completion but was not."
            )
        logger.debug(f"Registering task complete for task {data_task_id}")

        if plan_id is not None:
            if plan_id not in task_node.plan_to_child_block_lineages:
                raise ValueError(
                    f"Expected plan {plan_id} to be registered before task "
                    "completion but was not. There should never be more than "
                    "one instance of a reconstruction targeting the same task "
                    "in flight at a time. Retries of reconstructions should "
                    "pass the same plan ID to the same task."
                )
            del task_node.plan_to_child_block_lineages[plan_id]

    def register_failed_task(
        self, data_task_id: str, plan_id: Optional[str] = None
    ) -> Tuple[List[str], str]:
        """
        Mark a task as failed and begin reconstruction of it and its lineage.

        Args:
            data_task_id: The ID of the data task that failed.
            plan_id: The plan ID of the failed task if the task was a
                re-execution attempt. If not provided, the task is assumed to be
                the target of reconstruction from a fresh attempt.

        Returns:
            A tuple of the data task IDs of the seed tasks to resubmit for
            reconstruction, sorted to keep the order deterministic, and the plan
            ID associated with this reconstruction attempt.

        Raises:
            ValueError: If the task, or a child of a task along the failed path,
                is not registered.
                Invariant: the task must always be registered before a terminal
                state is reached.
        """
        task_node = self._data_task_id_to_task_node.get(data_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before getting "
                "a failure status but was not."
            )
        logger.debug(
            f"Registering failed task for task {data_task_id} with plan id {plan_id}"
        )

        seed_task_ids: Set[str] = set()
        # Maps a task's data task ID to the IDs of the children it has already
        # been traced from, so that every lineage edge is walked at most once.
        traced_child_ids: Dict[str, Set[str]] = {}

        plan_id_to_attach: str = data_task_id if plan_id is None else plan_id

        def _trace_parent_for_reconstruction(
            node: TaskNode, child_node: Optional[TaskNode] = None
        ) -> None:
            # Sibling failures in the same fan-out trace back to a shared
            # ancestor. That ancestor only needs to re-run once to serve all of
            # its pending children, so an edge that was already traced must not
            # be traced -- and its seed resubmitted -- a second time.
            child_ids = traced_child_ids.setdefault(node.data_task_id, set())
            if child_node is not None:
                if child_node.data_task_id in child_ids:
                    return
                child_ids.add(child_node.data_task_id)

            # Every task along the failed path takes part in the plan. The target
            # of the reconstruction has no child to re-produce blocks for, so its
            # lineage for the plan stays empty.
            child_block_lineages = node.plan_to_child_block_lineages.setdefault(
                plan_id_to_attach, []
            )

            if child_node is not None:
                if child_node.data_task_id not in node.child_task_block_dependencies:
                    raise ValueError(
                        "Failed to construct reconstruction plan for "
                        f"{data_task_id}. Expected child task "
                        f"{child_node.data_task_id} to be registered before "
                        "getting a failure status but was not."
                    )
                for output_index in node.child_task_block_dependencies[
                    child_node.data_task_id
                ]:
                    child_block_lineages.append(
                        ChildBlockDependency(
                            child_data_task_id=child_node.data_task_id,
                            output_index=output_index,
                        )
                    )
            logger.debug(
                f"Task {node.data_task_id} has the following plan: "
                f"{node.plan_to_child_block_lineages}"
            )
            if len(node.parent_tasks) == 0:
                # Idempotent: a seed reached through multiple paths is recorded
                # only once.
                seed_task_ids.add(node.data_task_id)
            else:
                for parent_task_node in node.parent_tasks:
                    _trace_parent_for_reconstruction(parent_task_node, node)

        _trace_parent_for_reconstruction(task_node)
        # Sort to keep the order of the seed task IDs deterministic.
        return sorted(seed_task_ids), plan_id_to_attach

    def get_pending_children(
        self, data_task_id: str, plan_id: str
    ) -> Dict[str, Dict[str, List[int]]]:
        """
        Get the children that must be reconstructed for the given data task.

        Children that are already in the middle of re-executing a reconstruction
        are not included.

        Args:
            data_task_id: The ID of the data task to get the pending children for.
            plan_id: The ID of the plan to get the pending children for.

        Returns:
            A mapping of each pending child task ID to a mapping of parent task
            ID to the indices of the output blocks that parent produced and the
            child task depends on.

        Raises:
            ValueError: If the task is not already registered.
                        Invariant: only registered tasks and their children can \
                        be reconstructed.
        """
        task_node = self._data_task_id_to_task_node.get(data_task_id)
        if task_node is None:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before getting "
                "pending children but was not."
            )

        child_block_lineages = task_node.plan_to_child_block_lineages.get(plan_id, [])
        child_ids_in_plan = {
            child_block_lineage.child_data_task_id
            for child_block_lineage in child_block_lineages
        }

        pending_children: Dict[str, Dict[str, List[int]]] = {}
        for child_task_node in task_node.child_tasks:
            # Only consider children that take an output produced by the plan.
            if child_task_node.data_task_id not in child_ids_in_plan:
                continue

            dependencies_by_parent: Dict[str, List[int]] = {}
            for parent_task_node in child_task_node.parent_tasks:
                output_indices = parent_task_node.child_task_block_dependencies[
                    child_task_node.data_task_id
                ]
                dependencies_by_parent[parent_task_node.data_task_id] = output_indices
            pending_children[child_task_node.data_task_id] = dependencies_by_parent

        logger.debug(
            f"Pending children for task {data_task_id} with plan: {plan_id} -> "
            f"{child_block_lineages}"
        )
        return pending_children

    def get_object_reuse_status(
        self, data_task_id: str, output_index: int, plan_id: str
    ) -> ObjectReuseStatus:
        """
        Get the reuse status of one output block of the given task.

        See :class:`ObjectReuseStatus` for the meaning of each status.

        Args:
            data_task_id: The ID of the data task that produced the output.
            output_index: The index of the output object to get the status for.
            plan_id: The ID of the plan to get the status for.

        Returns:
            The reuse status of the output block at ``output_index`` for the
            reconstruction attempt keyed by ``plan_id``.

        Raises:
            ValueError: If the task is not already registered.
        """
        node = self._data_task_id_to_task_node.get(data_task_id)
        if node is None:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before getting "
                "object reuse status but was not."
            )
        task_node: TaskNode = node

        # A plan is keyed by the data task ID of the task whose failure opened it.
        is_reconstruction_target = plan_id == data_task_id

        def _log_and_return(status: ObjectReuseStatus) -> ObjectReuseStatus:
            logger.debug(
                f"Object reuse status for task {data_task_id} at index "
                f"{output_index} with plan {plan_id} "
                f"(target={is_reconstruction_target}) -> "
                f"{task_node.plan_to_child_block_lineages.get(plan_id, [])} "
                f"gives {status.name}"
            )
            return status

        if not is_reconstruction_target:
            if plan_id not in task_node.plan_to_child_block_lineages:
                return _log_and_return(ObjectReuseStatus.OBJECT_UNRELATED)
            # For all non-target tasks in the plan, only the outputs that the
            # plan needs should be reconstructed.
            for block_lineage in task_node.plan_to_child_block_lineages[plan_id]:
                if block_lineage.output_index == output_index:
                    return _log_and_return(ObjectReuseStatus.OBJECT_REUSED)
            return _log_and_return(ObjectReuseStatus.OBJECT_PRUNED)

        # A child task fetched the block to its own node, so a copy of these rows
        # outlives the node that produced them and re-producing would duplicate.
        #
        # Note what does *not* count: an output sitting in a downstream operator's
        # queue. That queue holds an `ObjectRef`, so the only copy is still on the
        # producing node and dies with it -- such an output falls through to
        # OBJECT_NEW below and is re-emitted, which is required, not a duplicate.
        object_used_by_child = any(
            output_index in output_indices
            for output_indices in task_node.child_task_block_dependencies.values()
        )
        if object_used_by_child:
            return _log_and_return(ObjectReuseStatus.OBJECT_PRUNED)

        # For the task that is the target of reconstruction, its unconsumed
        # outputs can safely be taken by anyone, since the previously produced and
        # unconsumed outputs must have died with the previous node. (Assumes
        # reconstruction happens only upon node deaths.)
        return _log_and_return(ObjectReuseStatus.OBJECT_NEW)
