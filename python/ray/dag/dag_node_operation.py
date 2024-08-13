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
        local_idx: int,
        operation_type: _DAGNodeOperationType,
    ):
        """
        Args:
            local_idx: The index of the task that this operation belongs to
                in the actor's ExecutableTask list. The index is not the same
                as bind_index because there may be more tasks bound to an actor
                than tasks that appear in the current compiled DAG.
            operation_type: The type of operation to perform.
        """
        self.local_idx = local_idx
        self.type = operation_type


@total_ordering
class _DAGOperationGraphNode:
    def __init__(
        self,
        operation: _DAGNodeOperation,
        dag_idx: int,
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
            dag_idx: A unique index which can be used to index into
                `CompiledDAG.idx_to_task` to get the corresponding task.
            actor_handle: The actor handle to which this operation belongs.
            requires_nccl: Whether this operation requires NCCL.
        """
        self.operation = operation
        self.dag_idx = dag_idx
        self.actor_handle = actor_handle
        self.requires_nccl = requires_nccl
        # The in_edges and out_edges are sets of tuples. Each tuple contains
        # an integer `dag_idx`, which can be used to index into `idx_to_task`
        # to get the corresponding task, and a `_DAGNodeOperationType`, which can
        # be READ, COMPUTE, or WRITE.
        self.in_edges: Set[Tuple[int, _DAGNodeOperationType]] = set()
        self.out_edges: Set[Tuple[int, _DAGNodeOperationType]] = set()

    @property
    def in_degree(self) -> int:
        return len(self.in_edges)

    def __lt__(self, other: "_DAGOperationGraphNode"):
        """
        Two _DAGOperationGraphNodes are comparable only when they belong to
        the same actor. For operations on the same actor, if `local_idx` is smaller,
        the DAGNode to which this operation belongs has a smaller `bind_index`.
        """
        assert self.actor_handle == other.actor_handle
        return self.operation.local_idx < other.operation.local_idx

    def __eq__(self, other: "_DAGOperationGraphNode"):
        """
        Two _DAGOperationGraphNodes are comparable only when they belong to the
        same actor. For operations on the same actor, two operations are equal
        only when they have the same `local_idx` and `type`.
        """
        assert self.actor_handle == other.actor_handle
        return (
            self.operation.local_idx == other.operation.local_idx
            and self.operation.type == other.operation.type
        )

    def __hash__(self):
        """
        An operation is uniquely identified by its `dag_idx` and type.
        """
        return hash((self.operation, self.dag_idx))


def _add_edge(from_node: _DAGOperationGraphNode, to_node: _DAGOperationGraphNode):
    """
    Add an edge from `from_node` to `to_node`. An edge is a tuple of
    the operation's `dag_idx` and type.
    """
    from_node.out_edges.add((to_node.dag_idx, to_node.operation.type))
    to_node.in_edges.add((from_node.dag_idx, from_node.operation.type))


def _select_next_nodes(
    actor_to_candidates: Dict["ray._raylet.ActorID", List[_DAGOperationGraphNode]],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
):
    """
    This function selects the next nodes for topological sort to generate execution
    schedule. If there are multiple candidate _DAGOperationGraphNodes, select nodes
    based on the following rules:

    #1  If the nodes are not NCCL write nodes, select the one with the smallest
        `bind_index`. If there are multiple candidate nodes with the smallest
        `bind_index` among the actors to which they belong, any one of them is
        acceptable, but the implementation ensures the result is deterministic.
        For the implementation details, we maintain a priority queue for each actor,
        where the head of the priority queue is the node with the smallest `bind_index`.
    #2  If #1 cannot be satisfied, it means that all candidate nodes are NCCL write
        nodes. In this case, select the one at the head of the priority queue and
        its immediately downstream nodes, which are NCCL read nodes, regardless of
        whether the downstream nodes are heads of their own priority queues.

    This function may return multiple nodes if they are NCCL nodes. In that case,
    this function only removes the NCCL write node, which is also the head of a
    priority queue. Other nodes will be removed in the following iterations. The
    NCCL read nodes will be returned even though they should not yet be in the
    candidate list.

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
    next_nodes: List[_DAGOperationGraphNode] = []
    for _, candidates in actor_to_candidates.items():
        if not (
            candidates[0].requires_nccl
            and candidates[0].operation.type == _DAGNodeOperationType.WRITE
        ):
            next_nodes.append(heapq.heappop(candidates))
            assert len(next_nodes) == 1
            return next_nodes

    first_nccl_node: Optional[_DAGOperationGraphNode] = None
    for _, candidates in actor_to_candidates.items():
        if (
            candidates[0].requires_nccl
            and candidates[0].operation.type == _DAGNodeOperationType.WRITE
        ):
            first_nccl_node = candidates[0]
            break

    assert first_nccl_node is not None
    next_nodes.append(
        heapq.heappop(actor_to_candidates[first_nccl_node.actor_handle._actor_id])
    )

    # An NCCL write node is picked. NCCL is a blocking operation, so we need to pick all
    # the corresponding NCCL read nodes to avoid a deadlock.
    for downstream_node_metadata in first_nccl_node.out_edges:
        dag_idx, op_type = downstream_node_metadata[0], downstream_node_metadata[1]
        downstream_node = graph[dag_idx][op_type]
        assert downstream_node.operation.type == _DAGNodeOperationType.READ
        next_nodes.append(downstream_node)
    assert len(next_nodes) == 1 + len(first_nccl_node.out_edges)
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
        idx_to_task: A dictionary that maps the `dag_idx` to the `CompiledTask`.
            `CompiledTask` contains information about a DAGNode and its downstream
            nodes.

        actor_to_operation_nodes: A dictionary that maps an actor handle to
            a list of lists of _DAGOperationGraphNode. For the same actor, the
            index of the outer list corresponds to the index of the ExecutableTask
            in the list of `executable_tasks` in `actor_to_executable_tasks`. In
            the inner list, the order of operations is READ, COMPUTE, and WRITE.

    Returns:
        A graph where each node is a _DAGOperationGraphNode. The key is `dag_idx`,
        the index to retrieve its task from `idx_to_task`, and the value is a
        dictionary that maps the _DAGNodeOperationType (READ, COMPUTE, or WRITE)
        to the corresponding _DAGOperationGraphNode
    """
    assert idx_to_task
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]] = {}

    for _, operation_nodes_list in actor_to_operation_nodes.items():
        prev_compute_node = None
        for operation_nodes in operation_nodes_list:
            dag_idx = operation_nodes[0].dag_idx
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
            assert dag_idx not in graph
            graph[dag_idx] = {
                _DAGNodeOperationType.READ: read_node,
                _DAGNodeOperationType.COMPUTE: compute_node,
                _DAGNodeOperationType.WRITE: write_node,
            }

    # Import `ray.dag` here to avoid circular import.
    from ray.dag import ClassMethodNode, MultiOutputNode

    # Add an edge from WRITE of the writer task to READ of the reader task.
    for dag_idx, task in idx_to_task.items():
        if not isinstance(task.dag_node, ClassMethodNode):
            # The graph is used to generate an execution schedule for each actor.
            # The edge from the InputNode has no impact on the final execution
            # schedule.
            continue
        for downstream_dag_idx in task.downstream_node_idxs:
            downstream_dag_node = idx_to_task[downstream_dag_idx].dag_node
            if isinstance(downstream_dag_node, MultiOutputNode):
                continue
            _add_edge(
                graph[dag_idx][_DAGNodeOperationType.WRITE],
                graph[downstream_dag_idx][_DAGNodeOperationType.READ],
            )
    return graph


def _generate_actor_to_execution_schedule(
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]]
):
    """
    Generate an execution schedule for each actor. The schedule is a list of
    operations to be executed. The function uses a topological sort algorithm
    to generate the schedule.

    Args:
        graph: A graph where each node is a _DAGOperationGraphNode. The key is
            `dag_idx`, the index to retrieve its task from `idx_to_task`, and
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

    # Topological sort
    while actor_to_candidates:
        # The function `_select_next_nodes` will pop a candidate node from
        # `actor_to_candidates` and return a list of nodes that can be executed
        # in the next step. If multiple nodes are returned, only the NCCL write
        # node is popped in this iteration.
        nodes = _select_next_nodes(actor_to_candidates, graph)
        for node in nodes:
            if node in visited_nodes:
                continue
            actor_to_execution_schedule[node.actor_handle].append(node.operation)
            visited_nodes.add(node)
            for out_node_dag_idx, out_node_type in node.out_edges:
                out_node = graph[out_node_dag_idx][out_node_type]
                out_node.in_edges.remove((node.dag_idx, node.operation.type))
                if out_node.in_degree == 0:
                    heapq.heappush(
                        actor_to_candidates[out_node.actor_handle._actor_id],
                        out_node,
                    )

        delete_keys = []
        for actor_id, candidates in actor_to_candidates.items():
            if len(candidates) == 0:
                delete_keys.append(actor_id)
        for key in delete_keys:
            del actor_to_candidates[key]
    return actor_to_execution_schedule
