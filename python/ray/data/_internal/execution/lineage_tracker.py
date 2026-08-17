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
class Edge:
    """
    A direction agnostic edge represented by a tuple of the
    data task id associated with the block output. Data task id can be
    either the parent id or the child id. The output index is always the
    index of the output produced by the parent that the child task depends on.
    """

    data_task_id: str
    output_index: int


@dataclass
class TaskNode:
    """
    A node within the lineage graph tracking the child and parents of a task.
    Note: The data task id represents the id of the initial task execution. All
    retry of the initial task should be represented by the same data task id even
    if the ray core task id differs.
    """

    data_task_id: str
    parent_task: Optional["TaskNode"]
    child_task: Optional["TaskNode"]
    # The indecies of outputs produced by this block that the child task depends on.
    child_task_block_dependencies: List[int]


class LineageTracker:
    def __init__(self):
        self._data_task_id_to_task_node: Dict[str, TaskNode] = {}

    def register_task_submission(
        self, data_task_id: str, dependencies: List[Edge]
    ) -> None:
        """
        Register a newly submitted task with the lineage graph.
        Repeated registration of the same data task id will be ignored.

        Args:
            data_task_id: The id of the data task that was submitted.
            dependencies: The blocks that the task depends on.
                          A tuple of the parent task id and the output index
                          of the parent that the child task depends on.

        Raises:
            ValueError: If the parent task is not registered.
                        Invariant: A task can only be submitted if all its parents have been submitted.
        """

        if data_task_id in self._data_task_id_to_task_node:
            return

        # construct child to parent edge.
        if len(dependencies) == 0:
            parent_task_node = None
        else:
            parent_task_node = self._data_task_id_to_task_node.get(
                dependencies[0].data_task_id
            )
            if parent_task_node is None:
                raise ValueError(
                    f"Expected parent task {dependencies[0].data_task_id} to be registered before child task {data_task_id} but was not."
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
            data_task_id: The id of the data task that was completed.

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
        has begun reconstruction. Returns the data task id associated
        with the seed task to be resubmitted for reconstruction.

        Args:
            data_task_id: The id of the data task that was failed.

        Returns:
            The data task id associated with the seed task to be resubmitted for reconstruction.

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

        def _trace_parent_for_reconstruction(task_node: TaskNode):
            if task_node.parent_task is None:
                return task_node.data_task_id
            else:
                return _trace_parent_for_reconstruction(task_node.parent_task)

        return _trace_parent_for_reconstruction(task_node)

    def get_pending_children(self, data_task_id: str) -> Dict[str, List[int]]:
        """
        Get the child that needs to be reconstructed for the task associated with the given data task id.
        Children that already are in the middle of re-executing a reconstruction are not included.
        Returns a mapping of the child task id to the indecies of outputs produced by the given
        task that the child task depends on.

        Args:
            data_task_id: The id of the data task to get the pending children for.
        Returns:
            A mapping of the child task id to the indecies of outputs produced by the given
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
        OOBJECT_PRUNED -> object should be ignored and gc'd
        OBJECT_NEW -> object is unseen and should be submitted to a fresh task
        OBJECT_REUSED -> object should be resubmitted for reconstruction attempt
        OBJECT_UNRELATED -> object is unrelated to this reconstruction attempt.
                            we might want to raise on this status when handling it outside of testing.

        Args:
            data_task_id: The id of the data task to get the object reuse status for.
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
        object_used_by_child = False
        for dependency_output_index in task_node.child_task_block_dependencies:
            if dependency_output_index == output_index:
                object_used_by_child = True

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
