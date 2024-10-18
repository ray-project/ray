from functools import total_ordering
from enum import Enum
from typing import Optional, Tuple, List, Dict
import copy
import logging
import ray
import heapq
from collections import defaultdict


logger = logging.getLogger(__name__)


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

    def __str__(self):
        if self == _DAGNodeOperationType.READ:
            return "R"
        elif self == _DAGNodeOperationType.COMPUTE:
            return "C"
        elif self == _DAGNodeOperationType.WRITE:
            return "W"
        assert False, f"Unknown operation type: {self}"


class _DAGNodeOperation:
    def __init__(
        self,
        exec_task_idx: int,
        operation_type: _DAGNodeOperationType,
        method_name: Optional[str] = None,
    ):
        """
        Args:
            exec_task_idx: The index of the task that this operation belongs to
                in the actor's ExecutableTask list. The index is not the same
                as bind_index because there may be more tasks bound to an actor
                than tasks that appear in the current compiled DAG.
            operation_type: The type of operation to perform.
            method_name: The name of the method that this operation originates
                from. This is only for debugging purposes.
        """
        self.exec_task_idx = exec_task_idx
        self.type = operation_type
        self.method_name = method_name

    def __repr__(self):
        return f"(Task idx: {self.exec_task_idx}, Type: {self.type})"

    def __str__(self):
        return f"([{self.exec_task_idx}] {self.method_name} {self.type})"

    def __hash__(self):
        return hash((self.exec_task_idx, self.type))

    def __eq__(self, other):
        # An operation is uniquely identified by its `exec_task_idx` and type.
        # `func_name` is only for debugging purposes.
        return self.exec_task_idx == other.exec_task_idx and self.type == other.type


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
        # The in_edges and out_edges are dicts of tuples to strings.
        # Each tuple (the key) contains an integer `task_idx`, which can be
        # used to index into `idx_to_task` to get the corresponding task,
        # and a `_DAGNodeOperationType`, which can be READ, COMPUTE, or WRITE.
        # The string (the value) is the label of the edge, which will be used
        # to annotate the edge in the visualization of the execution schedule.
        self.in_edges: Dict[Tuple[int, _DAGNodeOperationType], str] = {}
        self.out_edges: Dict[Tuple[int, _DAGNodeOperationType], str] = {}

    @property
    def in_degree(self) -> int:
        return len(self.in_edges)

    def __lt__(self, other: "_DAGOperationGraphNode"):
        """
        This function defines the order of the nodes in the priority queue used in
        `_select_next_nodes`. The priority queue is a min-heap, so the node with
        higher priority is considered "less than" the other node.
        """
        # If two nodes belong to the same actor, select the one with
        # the smaller `exec_task_idx`.
        if self.actor_handle == other.actor_handle:
            return self.operation.exec_task_idx < other.operation.exec_task_idx
        # If two nodes belong to different actors and one of them is an NCCL
        # write node, select the one that is not an NCCL write node.
        is_nccl_write = (
            self.operation.type == _DAGNodeOperationType.WRITE and self.requires_nccl
        )
        other_is_nccl_write = (
            other.operation.type == _DAGNodeOperationType.WRITE and other.requires_nccl
        )
        if is_nccl_write != other_is_nccl_write:
            return not is_nccl_write
        # If two nodes belong to different actors and both are either NCCL write
        # nodes or neither are NCCL write nodes, select the one with the smaller
        # `exec_task_idx`. If they have the same `exec_task_idx`, select the one
        # with the smaller `task_idx`.
        if self.operation.exec_task_idx != other.operation.exec_task_idx:
            return self.operation.exec_task_idx < other.operation.exec_task_idx
        return self.task_idx < other.task_idx

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

    def __str__(self):
        class_name = (
            self.actor_handle._ray_actor_creation_function_descriptor.class_name
        )
        actor_id = self._actor_id.hex()
        actor_id_abbv = actor_id[:4] + "..."
        return (
            class_name
            + "_"
            + actor_id_abbv
            + f" [{self.operation.exec_task_idx}] "
            + f"{self.operation.method_name} {self.operation.type}"
        )

    @property
    def _actor_id(self):
        return self.actor_handle._ray_actor_id.hex()


def _add_edge(
    from_node: _DAGOperationGraphNode, to_node: _DAGOperationGraphNode, label: str = ""
):
    """
    Add an edge from `from_node` to `to_node`. An edge is a tuple of
    the operation's `task_idx` and type.

    Args:
        from_node: The node from which the edge originates.
        to_node: The node to which the edge points.
        label: The label of the edge. This will be used to annotate the edge
            in the visualization of the execution schedule.
    """
    from_node.out_edges[(to_node.task_idx, to_node.operation.type)] = label
    to_node.in_edges[(from_node.task_idx, from_node.operation.type)] = label


def _select_next_nodes(
    actor_to_candidates: Dict["ray._raylet.ActorID", List[_DAGOperationGraphNode]],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
):
    """
    This function selects the next nodes for topological sort to generate execution
    schedule. If there are multiple candidate _DAGOperationGraphNodes, select the node
    with the top priority based on the following rules:

    #1  If two candidate nodes belong to the same actor, select the one with
        the smaller `exec_task_idx`.

    #2  If two candidate nodes belong to different actors and both are either NCCL
        write nodes or neither are NCCL write nodes, select the one with the smaller
        `exec_task_idx`. If they have the same `exec_task_idx`, select the one with the
        smaller `task_idx`.

    #3  If two candidate nodes belong to different actors and one of them is an NCCL
        write node, select the one that is not an NCCL write node.

    For the implementation details, we maintain a priority queue for each actor,
    where the head of the priority queue is the node with the smallest `exec_task_idx`.

    If the selected node is an NCCL write node, select all its immediately downstream
    nodes, which are NCCL read nodes, regardless of whether the downstream nodes are
    heads of their own priority queues. In that case, this function only removes the
    NCCL write node, which is also the head of a priority queue. Other nodes will be
    removed in the following iterations. The NCCL read nodes will be returned even
    though they should not yet be in the candidate list.

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
    top_priority_node = None
    next_nodes: List[_DAGOperationGraphNode] = []
    for _, candidates in actor_to_candidates.items():
        if len(candidates) == 0:
            continue
        if top_priority_node is None or candidates[0] < top_priority_node:
            top_priority_node = candidates[0]
    assert top_priority_node is not None
    next_nodes.append(
        heapq.heappop(actor_to_candidates[top_priority_node.actor_handle._actor_id])
    )

    if not (
        top_priority_node.operation.type == _DAGNodeOperationType.WRITE
        and top_priority_node.requires_nccl
    ):
        assert len(next_nodes) == 1
        return next_nodes

    # An NCCL write node is picked. NCCL is a blocking operation, so we need to pick all
    # the corresponding NCCL read nodes to avoid a deadlock.
    for downstream_node_metadata in top_priority_node.out_edges:
        task_idx, op_type = downstream_node_metadata[0], downstream_node_metadata[1]
        downstream_node = graph[task_idx][op_type]
        assert downstream_node.operation.type == _DAGNodeOperationType.READ
        next_nodes.append(downstream_node)
    assert len(next_nodes) == 1 + len(top_priority_node.out_edges)
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
                _add_edge(prev_compute_node, compute_node, "next")
            prev_compute_node = compute_node
            assert task_idx not in graph
            graph[task_idx] = {
                _DAGNodeOperationType.READ: read_node,
                _DAGNodeOperationType.COMPUTE: compute_node,
                _DAGNodeOperationType.WRITE: write_node,
            }

    # Import `ray.dag` here to avoid circular import.
    from ray.dag import ClassMethodNode, MultiOutputNode

    # Add an edge from WRITE of the writer task to READ of the reader task.
    for task_idx, task in idx_to_task.items():
        if (
            isinstance(task.dag_node, ClassMethodNode)
            and task.dag_node.is_class_method_output
        ):
            # TODO(wxdeng): Handle the case where the task is a class method output.
            continue
        if not isinstance(task.dag_node, ClassMethodNode):
            # The graph is used to generate an execution schedule for each actor.
            # The edge from the InputNode has no impact on the final execution
            # schedule.
            continue
        for downstream_task_idx in task.downstream_task_idxs:
            downstream_dag_node = idx_to_task[downstream_task_idx].dag_node
            if (
                isinstance(downstream_dag_node, ClassMethodNode)
                and downstream_dag_node.is_class_method_output
            ):
                # TODO(wxdeng): Handle the case where the downstream task is
                # a class method output.
                continue
            if isinstance(downstream_dag_node, MultiOutputNode):
                continue
            _add_edge(
                graph[task_idx][_DAGNodeOperationType.WRITE],
                graph[downstream_task_idx][_DAGNodeOperationType.READ],
                "nccl"
                if graph[task_idx][_DAGNodeOperationType.WRITE].requires_nccl
                else "shm",
            )
    return graph


def _node_repr(node: _DAGOperationGraphNode, idx: int, optimized_index: int):
    """
    Representation of a node in the visualization of the execution schedule.

    Args:
        node: The node to be represented.
        idx: The index of the node in the execution schedule.
        optimized_index: The index of the node in the optimized execution schedule.
    """
    return str(node) + f" {idx},{optimized_index}"


def _visualize_execution_schedule(
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ],
    actor_to_optimized_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
):
    """
    Visualize the execution schedule for each actor.

    Args:
        actor_to_execution_schedule: A dictionary that maps an actor handle to
            the execution schedule which is a list of operation nodes.
        actor_to_optimized_schedule: A dictionary that maps an actor handle to the
            optimized execution schedule which is a list of operation nodes.
        graph: A graph where each node is a _DAGOperationGraphNode. The key is
            `task_idx`, the index to retrieve its task from `idx_to_task`, and
            the value is a dictionary that maps the _DAGNodeOperationType (READ,
            COMPUTE, or WRITE) to the corresponding _DAGOperationGraphNode. It is
            generated by `_build_dag_node_operation_graph`.
    """
    try:
        import graphviz
    except ImportError:
        raise ImportError(
            "Please install graphviz to visualize the execution schedule. "
            "You can install it by running `pip install graphviz`."
        )

    dot = graphviz.Digraph(comment="DAG")
    node_to_repr: Dict[_DAGOperationGraphNode, str] = {}

    for actor, execution_nodes in actor_to_execution_schedule.items():
        optimized_schedule = actor_to_optimized_schedule[actor]
        node_to_optimized_index = {node: i for i, node in enumerate(optimized_schedule)}

        with dot.subgraph(name=f"cluster_{execution_nodes[0]._actor_id}") as subgraph:
            subgraph.attr(rank=execution_nodes[0]._actor_id)
            for i, node in enumerate(execution_nodes):
                optimized_index = node_to_optimized_index.get(node)
                node_repr = _node_repr(node, i, optimized_index)
                color = "red" if optimized_index != i else "black"
                subgraph.node(node_repr, node_repr, color=color)
                node_to_repr[node] = node_repr

    for actor, execution_nodes in actor_to_execution_schedule.items():
        for i, node in enumerate(execution_nodes):
            node_repr = node_to_repr[node]
            for out_edge, label in node.out_edges.items():
                out_task_idx, out_op_type = out_edge
                out_node = graph[out_task_idx][out_op_type]
                out_node_repr = node_to_repr[out_node]
                color = "blue" if label == "nccl" else "black"
                dot.edge(node_repr, out_node_repr, label=label, color=color)

    logger.info(
        "Writing compiled graph schedule visualization "
        "to compiled_graph_schedule.png"
    )
    dot.render("compiled_graph_schedule", format="png", view=False)


def _generate_actor_to_execution_schedule(
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]]
) -> Dict["ray.actor.ActorHandle", List[_DAGOperationGraphNode]]:
    """
    Generate an execution schedule for each actor. The schedule is a list of
    operation nodes to be executed. The function uses a topological sort
    algorithm to generate the schedule.

    Args:
        graph: A graph where each node is a _DAGOperationGraphNode. The key is
            `task_idx`, the index to retrieve its task from `idx_to_task`, and
            the value is a dictionary that maps the _DAGNodeOperationType (READ,
            COMPUTE, or WRITE) to the corresponding _DAGOperationGraphNode. It is
            generated by `_build_dag_node_operation_graph`.

    Returns:
        actor_to_execution_schedule: A dictionary that maps an actor handle to
            the execution schedule which is a list of operation nodes to be
            executed.
    """

    # Mapping from the actor handle to the execution schedule which is a list
    # of operations to be executed.
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
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

    # Use topological sort algorithm to generate the execution schedule. Each iteration
    # pops a candidate node from `actor_to_candidates` and each DAG node consists of
    # three operations: READ, COMPUTE, and WRITE.
    for _ in range(len(graph) * 3):
        # The function `_select_next_nodes` will pop a candidate node from
        # `actor_to_candidates` and return a list of nodes that can be executed
        # in the next step. If multiple nodes are returned, only the NCCL write
        # node is popped in this iteration.
        nodes = _select_next_nodes(actor_to_candidates, graph)
        for node in nodes:
            if node in visited_nodes:
                continue
            actor_to_execution_schedule[node.actor_handle].append(node)
            visited_nodes.add(node)
            for out_node_task_idx, out_node_type in node.out_edges:
                out_node = graph[out_node_task_idx][out_node_type]
                out_node.in_edges.pop((node.task_idx, node.operation.type))
                if out_node.in_degree == 0:
                    heapq.heappush(
                        actor_to_candidates[out_node.actor_handle._actor_id],
                        out_node,
                    )
    for _, candidates in actor_to_candidates.items():
        assert len(candidates) == 0
    return actor_to_execution_schedule


def _optimize_execution_schedule(
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ],
    overlap_gpu_communication: bool,
) -> Dict["ray.actor.ActorHandle", List[_DAGOperationGraphNode]]:
    """
    Optimize the execution schedule by overlapping computation and communication.

    Args:
        actor_to_execution_schedule: A dictionary that maps an actor handle to
            the execution schedule which is a list of operations to be executed.
        overlap_gpu_communication: Whether to overlap GPU communication with
            computation. If False, the function will return the original execution
            schedule (i.e., actor_to_execution_schedule).
    """
    if not overlap_gpu_communication:
        return actor_to_execution_schedule

    actor_to_optimized_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ] = copy.deepcopy(actor_to_execution_schedule)
    for optimized_schedule in actor_to_optimized_schedule.values():
        for i in range(len(optimized_schedule)):
            if (
                optimized_schedule[i].operation.type == _DAGNodeOperationType.READ
                and optimized_schedule[i].requires_nccl
            ):
                # For each NCCL read operation (i.e., recv), scan backwards
                # to find the nearest compute node to swap with so that
                # the NCCL read operation can be overlapped with computation.
                for j in range(i - 1, -1, -1):
                    if (
                        optimized_schedule[j].operation.type
                        == _DAGNodeOperationType.COMPUTE
                    ):
                        # Found a desired compute operation, make the swap
                        nccl_read_node = optimized_schedule[i]
                        sublist = optimized_schedule[j:i]
                        optimized_schedule[j + 1 : i + 1] = sublist
                        optimized_schedule[j] = nccl_read_node
                        break
                    if (
                        optimized_schedule[j].operation.type
                        == _DAGNodeOperationType.READ
                        or optimized_schedule[j].operation.type
                        == _DAGNodeOperationType.WRITE
                    ) and (optimized_schedule[j].requires_nccl):
                        # Found a NCCL read/write operation, skip the optimiation to
                        # keep relative order of NCCL operations
                        break
    return actor_to_optimized_schedule


def _extract_execution_schedule(
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ]
) -> Dict["ray.actor.ActorHandle", List[_DAGNodeOperation]]:
    """
    Extract _DAGNodeOperation from _DAGOperationGraphNode in the schedule
    and discard unnecessary information.
    """
    return {
        actor: [node.operation for node in nodes]
        for actor, nodes in actor_to_execution_schedule.items()
    }
