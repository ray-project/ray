from functools import total_ordering
from enum import Enum
from typing import Set, Tuple, List, Dict, Optional
import ray
import heapq
from collections import defaultdict


class _DAGNodeOperationType(Enum):
    """
    There are three types of operations that a DAG node can perform:
    1. READ: Read from an input channel.
    2. COMPUTE: Execute the method corresponding to the node.
    3. WRITE: Write to an output channel.
    """

    READ = "READ"
    COMPUTE = "COMPUTE"
    WRITE = "WRITE"


class _DAGNodeOperation:
    def __init__(
        self,
        exec_task_idx: int,
        operation_type: _DAGNodeOperationType,
    ):
        """
        Args:
            exec_task_idx: The index of the task that this operation belongs to
                in the actor's ExecutableTask list. The index is not the same
                as bind_index because there may be more tasks bound to an actor
                than tasks that appear in the current compiled DAG.
            operation_type: The type of operation to perform.
        """
        self.exec_task_idx = exec_task_idx
        self.type = operation_type

    def __repr__(self):
        return (
            f"_DAGNodeOperation("
            f"exec_task_idx: {self.exec_task_idx}, "
            f" type: {self.type})"
        )


@total_ordering
class _DAGOperationGraphNode:
    def __init__(
        self,
        operation: _DAGNodeOperation,
        task_idx: int,
        actor_handle: "ray.actor.ActorHandle",
        requires_nccl: bool,
    ):
        """
        _DAGOperationGraphNode represents a node in the DAG operation graph.
        It contains information about the node's in-degree, out-degree, edges,
        and the operation it performs.

        Args:
            operation: The operation that this node performs. The operation
                can be a READ, COMPUTE, or WRITE operation.
            task_idx: A unique index which can be used to index into
                `CompiledDAG.idx_to_task` to get the corresponding task.
            actor_handle: The actor handle to which this operation belongs.
            requires_nccl: Whether this operation requires NCCL.
        """
        self.operation = operation
        self.task_idx = task_idx
        self.actor_handle = actor_handle
        self.requires_nccl = requires_nccl
        # The in_edges and out_edges are sets of tuples. Each tuple contains
        # an integer `task_idx`, which can be used to index into `idx_to_task`
        # to get the corresponding task, and a `_DAGNodeOperationType`, which can
        # be READ, COMPUTE, or WRITE.
        self.in_edges: Set[Tuple[int, _DAGNodeOperationType]] = set()
        self.out_edges: Set[Tuple[int, _DAGNodeOperationType]] = set()
        # The collective group is a set of nodes that belong to the same
        # collective operation. Each node is represented by a tuple of its
        # task idx and type.
        self.collective_group: Set[Tuple[int, _DAGNodeOperationType]] = set()
        # The ready collective nodes are the nodes that are ready to be executed,
        # i.e., their in-degrees are zero. When a collective node is ready, it
        # will be added to the ready collective nodes of all the nodes in its
        # collective group.
        self.ready_collective_nodes: Set[Tuple[int, _DAGNodeOperationType]] = set()

    def __repr__(self):
        return (
            f"_DAGOperationGraphNode("
            f"operation: {self.operation}, "
            f"task_idx: {self.task_idx}, "
            f"actor_handle: {self.actor_handle}, "
            f"requires_nccl: {self.requires_nccl})"
        )

    def __lt__(self, other: "_DAGOperationGraphNode"):
        """
        This function defines the order of the nodes in the priority queue used in
        `_select_next_nodes`. The priority queue is a min-heap, so the node with
        higher priority is considered "less than" the other node.
        """

        # Compare two nodes based on the following rules:
        # 1. If lhs and rhs have different `exec_task_idx`, the one with the smaller
        #    `exec_task_idx` has higher priority.
        # 2. If lhs and rhs have the same `exec_task_idx`, the one with the smaller
        #    `task_idx` has higher priority.
        def compare(lhs: "_DAGOperationGraphNode", rhs: "_DAGOperationGraphNode"):
            if lhs.operation.exec_task_idx != rhs.operation.exec_task_idx:
                return lhs.operation.exec_task_idx < rhs.operation.exec_task_idx
            return lhs.task_idx < rhs.task_idx

        if self.actor_handle == other.actor_handle:
            # When both nodes belong to the same actor, use the default comparison.
            return compare(self, other)
        else:
            # Both nodes belong to different actors.
            if self.is_nccl_op != other.is_nccl_op:
                # When one node is an NCCL operation and the other is not, prioritize
                # the non-NCCL operation.
                return not self.is_nccl_op
            else:
                # When either both nodes are NCCL operations or both nodes are not
                # NCCL operations, use the default comparison.
                return compare(self, other)

    def __eq__(self, other: "_DAGOperationGraphNode"):
        """
        Two operations are equal only when they have the same `exec_task_idx` and `type`
        and belong to the same actor.
        """
        return (
            self.actor_handle == other.actor_handle
            and self.operation.exec_task_idx == other.operation.exec_task_idx
            and self.operation.type == other.operation.type
        )

    def __hash__(self):
        """
        An operation is uniquely identified by its `task_idx` and type.
        """
        return hash((self.operation, self.task_idx))

    @property
    def in_degree(self) -> int:
        return len(self.in_edges)

    @property
    def is_ready(self) -> bool:
        """
        If the node is not an NCCL compute, it is ready when it has a zero in-degree.
        If it is an NCCL compute, it is ready when all the nodes in its collective
        group have zero in-degrees.
        """
        if not self.is_nccl_compute:
            return self.in_degree == 0
        else:
            return len(self.ready_collective_nodes) == len(self.collective_group)

    @property
    def is_read(self) -> bool:
        return self.operation.type == _DAGNodeOperationType.READ

    @property
    def is_nccl_compute(self) -> bool:
        """
        A node is an NCCL compute if it is a compute node and requires NCCL, which
        is an NCCL collective operation.
        """
        return (
            self.operation.type == _DAGNodeOperationType.COMPUTE and self.requires_nccl
        )

    @property
    def is_nccl_write(self) -> bool:
        """
        A node is an NCCL write if it is a write node and requires NCCL.
        """
        return self.operation.type == _DAGNodeOperationType.WRITE and self.requires_nccl

    @property
    def is_nccl_op(self) -> bool:
        return self.is_nccl_compute or self.is_nccl_write


def _add_edge(from_node: _DAGOperationGraphNode, to_node: _DAGOperationGraphNode):
    """
    Add an edge from `from_node` to `to_node`. An edge is a tuple of
    the operation's `task_idx` and type.
    """
    from_node.out_edges.add((to_node.task_idx, to_node.operation.type))
    to_node.in_edges.add((from_node.task_idx, from_node.operation.type))


def _select_next_nodes(
    actor_to_candidates: Dict["ray._raylet.ActorID", List[_DAGOperationGraphNode]],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
) -> Optional[List[_DAGOperationGraphNode]]:
    """
    This function selects the next nodes for topological sort to generate execution
    schedule. If there are multiple candidate _DAGOperationGraphNodes, select the node
    with the top priority. The priority is defined in `_DAGOperationGraphNode.__lt__`.

    For the implementation details, we maintain a priority queue for each actor,
    where the head of the priority queue is the node with the smallest `exec_task_idx`.
    When a node has a zero in-degree, it is added to the corresponding actor's
    priority queue. For a node other than a NCCL compute node, it is ready to be
    executed if it has a zero in-degree. For an NCCL compute node, it is ready
    to be executed when all the nodes in its collective group have zero in-degrees.

    If a node is a NCCL compute node, it updates the `ready_collective_nodes` of
    all the nodes in its collective group. Unless all the nodes in its collective
    group have zero in-degrees, this node is removed from the candidate list.
    Eventually, exactly one NCCL compute node from its collective group is selected
    from the candidate list.

    If the selected node is an NCCL write node, select all the downstream NCCL
    read nodes. If the selected node is an NCCL compute node, select all the NCCL
    compute nodes in its collective group.

    Args:
        actor_to_candidates: A dictionary mapping an actor id to a list of
            candidate nodes. The list is maintained as a priority queue, so
            the head of the queue, i.e., `candidates[0]`, is the node with
            the smallest `bind_index`.
        graph: A dictionary mapping the index of a task to a dictionary of its
            _DAGOperationGraphNodes for different operations.

    Returns:
        A list of _DAGOperationGraphNodes to be placed into the corresponding
        execution schedules.
    """
    # If a node is a NCCL compute node, it updates the `ready_collective_nodes`
    # of all the nodes in its collective group. It is removed from the candidate
    # list if the collective group is not ready.
    for actor, candidates in actor_to_candidates.items():
        ready_candidates: List[_DAGOperationGraphNode] = []
        for node in candidates:
            if node.is_nccl_compute:
                for collective_node_metadata in node.collective_group:
                    task_idx, op_type = (
                        collective_node_metadata[0],
                        collective_node_metadata[1],
                    )
                    collective_node = graph[task_idx][op_type]
                    collective_node.ready_collective_nodes.add(
                        (node.task_idx, node.operation.type)
                    )
            if node.is_ready:
                ready_candidates.append(node)
        actor_to_candidates[actor] = ready_candidates

    top_priority_node = None
    for _, candidates in actor_to_candidates.items():
        if len(candidates) == 0:
            continue
        if top_priority_node is None or candidates[0] < top_priority_node:
            top_priority_node = candidates[0]

    if top_priority_node is None:
        return None
    next_nodes = [
        heapq.heappop(actor_to_candidates[top_priority_node.actor_handle._actor_id])
    ]

    if not top_priority_node.is_nccl_op:
        # A non-NCCL operation node is picked.
        assert len(next_nodes) == 1
    elif top_priority_node.is_nccl_write:
        # An NCCL write node is picked. NCCL is a blocking operation, so we need
        # to pick all the corresponding NCCL read nodes to avoid a deadlock.
        for downstream_node_metadata in top_priority_node.out_edges:
            task_idx, op_type = downstream_node_metadata[0], downstream_node_metadata[1]
            downstream_node = graph[task_idx][op_type]
            assert downstream_node.is_read
            next_nodes.append(downstream_node)
        assert len(next_nodes) == 1 + len(top_priority_node.out_edges)
    elif top_priority_node.is_nccl_compute:
        # An NCCL compute node is picked. NCCL is a blocking operation, so we need
        # to pick all the corresponding NCCL compute nodes in its collective group
        # to avoid a deadlock.
        for collective_node_metadata in top_priority_node.collective_group:
            task_idx, op_type = collective_node_metadata[0], collective_node_metadata[1]
            collective_node = graph[task_idx][op_type]
            assert collective_node.is_nccl_compute and collective_node.is_ready
            if collective_node != top_priority_node:
                next_nodes.append(collective_node)
        assert len(next_nodes) == len(top_priority_node.collective_group)

    return next_nodes


def _build_dag_node_operation_graph(
    idx_to_task: Dict[int, "ray.dag.compiled_dag_node.CompiledTask"],
    actor_to_operation_nodes: Dict[
        "ray.actor.ActorHandle", List[List[_DAGOperationGraphNode]]
    ],
) -> Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]]:
    """
    Generate a DAG node operation graph by adding edges based on the
    following rules:

    #1  Add edges from READ to COMPUTE, and from COMPUTE to WRITE, which
        belong to the same task.
    #2  Add an edge from COMPUTE with bind_index i to COMPUTE with bind_index
        i+1 if they belong to the same actor.
    #3  Add an edge from WRITE of the writer task to READ of the reader task.

    This is the step one of building an execution schedule for each actor.

    Args:
        idx_to_task: A dictionary that maps the `task_idx` to the `CompiledTask`.
            `CompiledTask` contains information about a DAGNode and its downstream
            nodes.

        actor_to_operation_nodes: A dictionary that maps an actor handle to
            a list of lists of _DAGOperationGraphNode. For the same actor, the
            index of the outer list corresponds to the index of the ExecutableTask
            in the list of `executable_tasks` in `actor_to_executable_tasks`. In
            the inner list, the order of operations is READ, COMPUTE, and WRITE.

    Returns:
        A graph where each node is a _DAGOperationGraphNode. The key is `task_idx`,
        the index to retrieve its task from `idx_to_task`, and the value is a
        dictionary that maps the _DAGNodeOperationType (READ, COMPUTE, or WRITE)
        to the corresponding _DAGOperationGraphNode
    """
    assert idx_to_task
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]] = {}

    for _, operation_nodes_list in actor_to_operation_nodes.items():
        prev_compute_node = None
        for operation_nodes in operation_nodes_list:
            task_idx = operation_nodes[0].task_idx
            read_node, compute_node, write_node = (
                operation_nodes[0],
                operation_nodes[1],
                operation_nodes[2],
            )
            # Add edges from READ to COMPUTE, and from COMPUTE to WRITE, which
            # belong to the same task.
            _add_edge(read_node, compute_node)
            _add_edge(compute_node, write_node)
            # Add an edge from COMPUTE with `bind_index` i to COMPUTE with
            # `bind_index` i+1 if they belong to the same actor.
            if prev_compute_node is not None:
                _add_edge(prev_compute_node, compute_node)
            prev_compute_node = compute_node
            assert task_idx not in graph
            graph[task_idx] = {
                _DAGNodeOperationType.READ: read_node,
                _DAGNodeOperationType.COMPUTE: compute_node,
                _DAGNodeOperationType.WRITE: write_node,
            }

    # Import `ray.dag` here to avoid circular import.
    from ray.dag import ClassMethodNode, CollectiveOutputNode, MultiOutputNode

    # Add an edge from WRITE of the writer task to READ of the reader task.
    for task_idx, task in idx_to_task.items():
        if not (
            isinstance(task.dag_node, ClassMethodNode)
            or isinstance(task.dag_node, CollectiveOutputNode)
        ):
            # The graph is used to generate an execution schedule for each actor.
            # The edge from the InputNode has no impact on the final execution
            # schedule.
            continue
        if (
            isinstance(task.dag_node, ClassMethodNode)
            and task.dag_node.is_class_method_output
        ):
            # TODO(wxdeng): Handle the case where the task is a class method output.
            continue
        for downstream_task_idx in task.downstream_task_idxs:
            downstream_dag_node = idx_to_task[downstream_task_idx].dag_node
            if isinstance(downstream_dag_node, MultiOutputNode):
                continue
            if (
                isinstance(downstream_dag_node, ClassMethodNode)
                and downstream_dag_node.is_class_method_output
            ):
                # TODO(wxdeng): Handle the case where the downstream task is
                # a class method output.
                continue
            _add_edge(
                graph[task_idx][_DAGNodeOperationType.WRITE],
                graph[downstream_task_idx][_DAGNodeOperationType.READ],
            )

    from ray.dag.collective_node import CollectiveGroup

    # Set collective group nodes for all the NCCL compute nodes.
    collective_group_to_task_idxs: Dict[CollectiveGroup, Set[int]] = defaultdict(set)
    for task_idx, task in idx_to_task.items():
        if isinstance(task.dag_node, CollectiveOutputNode):
            collective_group = task.dag_node.collective_group
            task_idxs = collective_group_to_task_idxs[collective_group]
            assert task_idx not in task_idxs
            task_idxs.add(task_idx)
    for task_idxs in collective_group_to_task_idxs.values():
        collective_group_nodes = {
            (task_idx, _DAGNodeOperationType.COMPUTE) for task_idx in task_idxs
        }
        for task_idx in task_idxs:
            collective_node = graph[task_idx][_DAGNodeOperationType.COMPUTE]
            collective_node.collective_group = collective_group_nodes

    return graph


def _generate_actor_to_execution_schedule(
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]]
) -> Dict["ray.actor.ActorHandle", List[_DAGNodeOperation]]:
    """
    Generate an execution schedule for each actor. The schedule is a list of
    operations to be executed. The function uses a topological sort algorithm
    to generate the schedule.

    Args:
        graph: A graph where each node is a _DAGOperationGraphNode. The key is
            `task_idx`, the index to retrieve its task from `idx_to_task`, and
            the value is a dictionary that maps the _DAGNodeOperationType (READ,
            COMPUTE, or WRITE) to the corresponding _DAGOperationGraphNode. It is
            generated by `_build_dag_node_operation_graph`.

    Returns:
        actor_to_execution_schedule: A dictionary that maps an actor handle to
            the execution schedule which is a list of operations to be executed.
    """

    # Mapping from the actor handle to the execution schedule which is a list
    # of operations to be executed.
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGNodeOperation]
    ] = defaultdict(list)

    # A dictionary mapping an actor id to a list of candidate nodes. The list
    # is maintained as a priority queue, so the head of the queue, i.e.,
    # `candidates[0]`, is the node with the smallest `bind_index`.
    actor_to_candidates: Dict[
        "ray._raylet.ActorID", List[_DAGOperationGraphNode]
    ] = defaultdict(list)
    for _, node_dict in graph.items():
        for _, node in node_dict.items():
            # A node with a zero in-degree edge means all of its dependencies
            # have been satisfied, including both data and control dependencies.
            # Therefore, it is a candidate for execution.
            if node.in_degree == 0:
                heapq.heappush(actor_to_candidates[node.actor_handle._actor_id], node)

    visited_nodes = set()

    # Use topological sort algorithm to generate the execution schedule.
    while True:
        # Select a list of nodes to be executed. There are three cases:
        # 1. If a selected node is not an NCCL operation, only itself is returned.
        # 2. If a selected node is an NCCL write operation, the corresponding NCCL
        #    read operations are also returned.
        # 3. If a selected node is an NCCL compute operation, all the nodes in its
        #    collective group are returned.
        # In cases 1 and 3, all the selected nodes are ready. In case 2, the NCCL
        # write node is ready, while the NCCL read nodes are not ready until their
        # in-degrees are updated.
        nodes = _select_next_nodes(actor_to_candidates, graph)
        if nodes is None:
            break
        # Add the selected nodes to the execution schedule.
        for node in nodes:
            if node in visited_nodes:
                continue
            actor_to_execution_schedule[node.actor_handle].append(node.operation)
            visited_nodes.add(node)
        # Update the in-degree of the downstream nodes.
        for node in nodes:
            for out_node_task_idx, out_node_type in node.out_edges:
                out_node = graph[out_node_task_idx][out_node_type]
                out_node.in_edges.remove((node.task_idx, node.operation.type))
                if out_node.in_degree == 0 and out_node not in visited_nodes:
                    # If the downstream node is already visited, it has been added to
                    # the execution schedule. They are the NCCL read nodes in case 2.
                    heapq.heappush(
                        actor_to_candidates[out_node.actor_handle._actor_id],
                        out_node,
                    )
        # All the selected nodes should be ready after updating the in-degrees.
        for node in nodes:
            assert node.is_ready, f"Expected {node} to be ready"
    assert len(visited_nodes) == len(graph) * 3, "Expected all nodes to be visited"
    for _, candidates in actor_to_candidates.items():
        assert len(candidates) == 0, "Expected all candidates to be empty"

    return actor_to_execution_schedule
