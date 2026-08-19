import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ObjectReuseStatus(Enum):
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
    A node within the lineage graph tracking the child and parents of a task.
    Note: The data task ID represents the ID of the initial task execution. All
    retries of the initial task should be represented by the same data task ID even
    if the Ray Core task ID differs.

    Note: Nodes hold references to their neighbors, so the graph is cyclic. The
    generated `__eq__` and `__repr__` are disabled in favor of implementations
    that only look at the neighbors' data task IDs and never traverse the graph.
    """

    data_task_id: str
    parent_task: Optional["TaskNode"]
    child_task: Optional["TaskNode"]
    # The output indices corresponding to blocks that the child task depends on.
    child_task_block_dependencies: List[int]

    def __repr__(self) -> str:
        parent_id = self.parent_task.data_task_id if self.parent_task else None
        child_id = self.child_task.data_task_id if self.child_task else None
        return (
            f"{type(self).__name__}(data_task_id={self.data_task_id!r}, "
            f"parent_task={parent_id!r}, child_task={child_id!r}, "
            f"child_task_block_dependencies={self.child_task_block_dependencies!r})"
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
        Repeated registration of the same data task ID will be ignored.

        Args:
            data_task_id: The ID of the data task that was submitted.
            dependencies: The blocks that the task depends on.
                          A tuple of the parent task ID and the output index
                          of the parent that the child task depends on.

        Raises:
            ValueError: If the parent task is not registered.
                        Invariant: A task can only be submitted if all its parents have been submitted.
        """

        logger.debug(
            f"Registering task submission for task {data_task_id} with dependencies {dependencies}"
        )
        if data_task_id in self._data_task_id_to_task_node:
            return

        # construct child to parent edge.
        if len(dependencies) == 0:
            parent_task_node = None
        else:
            # We assume this is a linear DAG so dependencies[0] holds.
            # We will remove this once we support fan-in and fan-outs.
            parent_task_node = self._data_task_id_to_task_node.get(
                dependencies[0].parent_data_task_id
            )
            if parent_task_node is None:
                raise ValueError(
                    f"Expected parent task {dependencies[0].parent_data_task_id} to be registered before child task {data_task_id} but was not."
                )
        dependency_indices: List[int] = []
        for dependency in dependencies:
            dependency_indices.append(dependency.output_index)

        task_node = TaskNode(
            data_task_id=data_task_id,
            parent_task=parent_task_node,
            child_task=None,
            child_task_block_dependencies=[],
        )
        self._data_task_id_to_task_node[data_task_id] = task_node

        # construct parent to child edge
        if parent_task_node is not None:
            parent_task_node.child_task = task_node
            parent_task_node.child_task_block_dependencies = dependency_indices

    def register_task_complete(self, data_task_id: str) -> None:
        """
        Update the task state for the given data task to completed.

        Note:
            On re-execution attempts, register_task_complete must only be called
            after dependencies of downstream tasks produced by this task are resolved.

        Args:
            data_task_id: The ID of the data task that was completed.

        Raises:
            ValueError: If the task is not already registered.
                        Invariant: The task must always be registered before a terminal state is reached.
        """
        if data_task_id not in self._data_task_id_to_task_node:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before completion but was not."
            )
        logger.debug(f"Registering task complete for task {data_task_id}")

    def register_failed_task(self, data_task_id: str) -> str:
        """
        Mark a task as failed and record that the task and its lienage
        has begun reconstruction. Returns the data task ID associated
        with the seed task to be resubmitted for reconstruction.

        Args:
            data_task_id: The ID of the data task that was failed.

        Returns:
            The data task ID associated with the seed task to be resubmitted for reconstruction.

        Raises:
            ValueError: If the task is not already registered.
                        Invariant: The task must always be registered before a terminal state is reached.
        """
        if data_task_id not in self._data_task_id_to_task_node:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before getting a failure status but was not."
            )
        task_node = self._data_task_id_to_task_node[data_task_id]
        logger.debug(f"Registering failed task for task {data_task_id}")

        curr_node = task_node
        while curr_node.parent_task is not None:
            curr_node = curr_node.parent_task
        return curr_node.data_task_id

    def get_pending_children(self, data_task_id: str) -> Dict[str, List[int]]:
        """
        Get the child that needs to be reconstructed for the task associated with the given data task ID.
        Children that already are in the middle of re-executing a reconstruction are not included.
        Returns a mapping of the child task ID to the indices of outputs produced by the given
        task that the child task depends on.

        Args:
            data_task_id: The ID of the data task to get the pending children for.
        Returns:
            A mapping of the child task ID to the indices of outputs produced by the given
            task that the child task depends on.

        Raises:
            ValueError: If the task is not already registered.
                        Invariant: Only registered tasks and its children can be reconstructed.
        """
        if data_task_id not in self._data_task_id_to_task_node:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before getting pending children but was not."
            )

        task_node = self._data_task_id_to_task_node[data_task_id]
        if task_node.child_task is None:
            return {}
        logger.debug(
            f"Pending children for task {data_task_id} -> {task_node.child_task.data_task_id}: {task_node.child_task_block_dependencies}"
        )
        return {
            task_node.child_task.data_task_id: task_node.child_task_block_dependencies
        }

    def get_object_reuse_status(
        self, data_task_id: str, output_index: int
    ) -> ObjectReuseStatus:
        """
        Get the reuse status for the output object of the given task at the associated output index.
        OBJECT_PRUNED -> object should be ignored and garbage collected
        OBJECT_NEW -> object is unseen and should be submitted to a fresh task
        OBJECT_REUSED -> object should be resubmitted for reconstruction attempt
        OBJECT_UNRELATED -> object is unrelated to this reconstruction attempt.
                            we might want to raise on this status when handling it outside of testing.

        Args:
            data_task_id: The ID of the data task to get the object reuse status for.
            output_index: The index of the output object to get the object reuse status for.

        Returns:
            The reuse status for the output object of the given task at the associated output index.

        Raises:
            ValueError: If the task is not already registered.
        """
        if data_task_id not in self._data_task_id_to_task_node:
            raise ValueError(
                f"Expected task {data_task_id} to be registered before getting object reuse status but was not."
            )

        task_node = self._data_task_id_to_task_node[data_task_id]

        # check if any child task depends on the output object
        object_used_by_child = output_index in task_node.child_task_block_dependencies

        if object_used_by_child:
            logger.debug(
                f"target node object reuse status for task {data_task_id} at index {output_index} gives OBJECT_REUSED"
            )
            return ObjectReuseStatus.OBJECT_REUSED

        # For the leaf task node that's the target of reconstruction, its unconsumed outputs
        # can be safely taken by anyone. Since the previously produced and unconsumed outputs
        # must have died in the previous node death. (Assumes reconstruction happens upon
        # only node deaths).
        logger.debug(
            f"target node object reuse status for task {data_task_id} at index {output_index} gives OBJECT_NEW"
        )
        return ObjectReuseStatus.OBJECT_NEW
