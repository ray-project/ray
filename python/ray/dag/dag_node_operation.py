from functools import total_ordering
from enum import Enum
from typing import Set, Tuple, List, Dict, Optional
import ray
import heapq


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
        idx: int,
        operation_type: _DAGNodeOperationType,
    ):
        """
        Args:
            idx: The index of the task that this operation belongs to
                in the actor's ExecutableTask list. The index is not
                the same as bind_index, but there are positive correlations
                between the two.
            operation_type: The type of operation to perform.
        """
        self.idx = idx
        self.type = operation_type


@total_ordering
class _DAGOperationGraphNode:
    def __init__(
        self,
        operation: _DAGNodeOperation,
        idx: int,
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
            idx: A unique index into the original DAG.
            dag_node: The DAGNode that this operation belongs to.
        """
        self.operation = operation
        self.idx = idx
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

    def __lt__(self, other):
        """
        Two _DAGOperationGraphNodes are comparable only when they belong to
        the same actor. For operations on the same actor, if idx is smaller,
        the DAGNode to which this operation belongs has a smaller `bind_index`.
        """
        assert self.actor_handle == other.actor_handle
        return self.operation.idx < other.operation.idx

    def __eq__(self, other):
        """
        Two _DAGOperationGraphNodes are comparable only when they belong to the
        same actor. For operations on the same actor, two operations are equal
        only when they have the same `idx` and `type`.
        """
        assert self.actor_handle == other.actor_handle
        return (
            self.operation.idx == other.operation.idx
            and self.operation.type == other.operation.type
        )

    def __hash__(self):
        return hash((self.operation, self.idx))

    def add_edge(self, out_node: "_DAGOperationGraphNode"):
        """
        Add an edge from this node to `out_node`. An edge is a tuple of
        the operation's index and type.
        """
        self.out_edges.add((out_node.idx, out_node.operation.type))
        out_node.in_edges.add((self.idx, self.operation.type))


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
    priority queue. Other nodes will be removed in the following iterations.

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
        global_idx, op_type = downstream_node_metadata[0], downstream_node_metadata[1]
        downstream_node = graph[global_idx][op_type]
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
        A graph where each node is a _DAGOperationGraphNode. The key is the index
        of the task in idx_to_task, and the value is a dictionary that maps the
        _DAGNodeOperationType (READ, COMPUTE, or WRITE) to the corresponding
        _DAGOperationGraphNode.
    """
    assert idx_to_task
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]] = {}

    for _, operation_nodes_list in actor_to_operation_nodes.items():
        prev_compute_node = None
        for operation_nodes in operation_nodes_list:
            idx = operation_nodes[0].idx
            read_node, compute_node, write_node = (
                operation_nodes[0],
                operation_nodes[1],
                operation_nodes[2],
            )
            # Add edges from READ to COMPUTE, and from COMPUTE to WRITE, which
            # belong to the same task.
            read_node.add_edge(compute_node)
            compute_node.add_edge(write_node)
            # Add an edge from COMPUTE with `bind_index` i to COMPUTE with
            # `bind_index` i+1 if they belong to the same actor.
            if prev_compute_node is not None:
                prev_compute_node.add_edge(compute_node)
            prev_compute_node = compute_node
            assert idx not in graph
            graph[idx] = {
                _DAGNodeOperationType.READ: read_node,
                _DAGNodeOperationType.COMPUTE: compute_node,
                _DAGNodeOperationType.WRITE: write_node,
            }

    # Import `ray.dag` here to avoid circular import.
    from ray.dag import ClassMethodNode, MultiOutputNode

    # Add an edge from WRITE of the writer task to READ of the reader task.
    for idx, task in idx_to_task.items():
        if not isinstance(task.dag_node, ClassMethodNode):
            # The graph is used to generate an execution schedule for each actor.
            # The edge from the InputNode has no impact on the final execution
            # schedule.
            continue
        for downstream_idx in task.downstream_node_idxs:
            downstream_dag_node = idx_to_task[downstream_idx].dag_node
            if isinstance(downstream_dag_node, MultiOutputNode):
                continue
            graph[idx][_DAGNodeOperationType.WRITE].add_edge(
                graph[downstream_idx][_DAGNodeOperationType.READ]
            )
    return graph
