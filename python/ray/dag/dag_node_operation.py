from functools import total_ordering
from enum import Enum
from typing import Set, Tuple, List, Dict, Optional
from ray.util.annotations import DeveloperAPI
import ray
import heapq


@DeveloperAPI
class DAGNodeOperationType(Enum):
    """
    There are three types of operations that a DAG node can perform:
    1. READ: Read from an input channel.
    2. COMPUTE: Execute the method corresponding to the node.
    3. WRITE: Write to an output channel.
    """

    READ = "READ"
    COMPUTE = "COMPUTE"
    WRITE = "WRITE"


@DeveloperAPI
class DAGNodeOperation:
    def __init__(
        self,
        idx: int,
        operation_type: DAGNodeOperationType,
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


@DeveloperAPI
@total_ordering
class DAGOperationGraphNode:
    def __init__(
        self,
        operation: DAGNodeOperation,
        idx: int,
        actor_handle: "ray.actor.ActorHandle",
        requires_nccl: bool,
    ):
        """
        DAGOperationGraphNode represents a node in the DAG operation graph.
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
        self.in_edges: Set[Tuple[int, DAGNodeOperationType]] = set()
        self.out_edges: Set[Tuple[int, DAGNodeOperationType]] = set()

    @property
    def in_degree(self) -> int:
        return len(self.in_edges)

    def __lt__(self, other):
        """
        Two DAGOperationGraphNodes are comparable only when they belong to
        the same actor. For operations on the same actor, if idx is smaller,
        the DAGNode to which this operation belongs has a smaller `bind_index`.
        """
        assert self.actor_handle == other.actor_handle
        return self.operation.idx < other.operation.idx

    def __eq__(self, other):
        """
        Two DAGOperationGraphNodes are comparable only when they belong to the
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

    def add_edge(self, out_node: "DAGOperationGraphNode"):
        """
        Add an edge from this node to `out_node`. An edge is a tuple of
        the operation's index and type.
        """
        self.out_edges.add((out_node.idx, out_node.operation.type))
        out_node.in_edges.add((self.idx, self.operation.type))


def _select_next_nodes(
    actor_to_candidates: Dict["ray._raylet.ActorID", List[DAGOperationGraphNode]],
    graph: Dict[int, Dict[DAGNodeOperationType, DAGOperationGraphNode]],
):
    """
    This function selects the next nodes for topological sort to generate execution
    schedule. If there are multiple DAGOperationGraphNodes with zero in-degree,
    select nodes based on the following rules:

    #1  If the nodes are not NCCL write nodes, select the one with the smallest
        `bind_index`. If there are multiple candidate nodes with the smallest
        `bind_index` of the actors that they belong to, any one of them is
        acceptable. For the implementation details, we maintain a priority queue
        for each actor, where the head of the priority queue is the node with the
        smallest `bind_index`.
    #2  If #1 cannot be satisfied, it means that all candidate nodes are NCCL write
        nodes. In this case, select the one at the head of the priority queue and
        its downstream nodes, which are NCCL read nodes, regardless of whether the
        downstream nodes are heads of their own priority queues.

    This function may return multiple nodes if they are NCCL nodes. In that case,
    this function only removes the NCCL write node, which is also the head of a
    priority queue. Other nodes will be removed in the following iterations.
    Additionally, visited_nodes ensures that the same node will not be scheduled
    more than once.

    Args:
        actor_to_candidates: A dictionary mapping an actor id to a list of
            candidate nodes with zero in-degree. The list is maintained as a
            priority queue, so the head of the queue, i.e., `candidates[0]`, is
            the node with the smallest `bind_index`.
        graph: A dictionary mapping the index of a task to a dictionary of its
            DAGOperationGraphNodes for different operations.

    Returns:
        A list of DAGOperationGraphNodes to be placed into the corresponding
        execution schedules.
    """
    next_nodes: List[DAGOperationGraphNode] = []
    first_nccl_node: Optional[DAGOperationGraphNode] = None
    for _, candidates in actor_to_candidates.items():
        if (
            not candidates[0].requires_nccl
            or candidates[0].operation.type != DAGNodeOperationType.WRITE
        ):
            next_nodes.append(heapq.heappop(candidates))
            assert len(next_nodes) == 1
            return next_nodes
        if first_nccl_node is None:
            assert candidates[0].requires_nccl
            first_nccl_node = candidates[0]

    assert first_nccl_node is not None
    assert first_nccl_node.operation.type == DAGNodeOperationType.WRITE
    next_nodes.append(
        heapq.heappop(actor_to_candidates[first_nccl_node.actor_handle._actor_id])
    )
    for downstream_node_metadata in first_nccl_node.out_edges:
        global_idx, op_type = downstream_node_metadata[0], downstream_node_metadata[1]
        downstream_node = graph[global_idx][op_type]
        next_nodes.append(downstream_node)
    assert len(next_nodes) == 1 + len(first_nccl_node.out_edges)
    return next_nodes
