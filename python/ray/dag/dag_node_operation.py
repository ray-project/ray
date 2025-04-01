from functools import total_ordering
from enum import Enum
from typing import Set, Tuple, List, Dict, Optional
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

    def viz_str(self):
        """
        A string representation of the operation type to be used in visualization.

        The result string is a single character because conciseness is preferred.
        """
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
                from. This is only for visualization and debugging purposes.
        """
        self.exec_task_idx = exec_task_idx
        self.type = operation_type
        self.method_name = method_name

    def __repr__(self):
        return (
            f"_DAGNodeOperation("
            f"exec_task_idx: {self.exec_task_idx}, "
            f" type: {self.type})"
        )

    def viz_str(self):
        """
        A string representation of the node to be used in visualization.
        """
        return f"[{self.exec_task_idx}] {self.method_name} {self.type.viz_str()}"

    def __hash__(self):
        return hash((self.exec_task_idx, self.type))

    def __eq__(self, other):
        # An operation is uniquely identified by its `exec_task_idx` and type.
        # `method_name` is only for debugging purposes.
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
        # The string (the value) is the visualization information of the edge,
        # it is a tuple of a label of the edge and a boolean indicating whether
        # the edge is a control dependency.
        self.in_edges: Dict[Tuple[int, _DAGNodeOperationType], Tuple[str, bool]] = {}
        self.out_edges: Dict[Tuple[int, _DAGNodeOperationType], Tuple[str, bool]] = {}
        # The collective nodes are the nodes that belong to the same collective
        # operation. Each node is represented by a tuple of its task idx and type.
        self.collective_idxs: Set[Tuple[int, _DAGNodeOperationType]] = set()
        # The ready collective nodes are the nodes that are ready to be executed,
        # i.e., their in-degrees are zero. When a collective node is ready, it
        # will be added to the ready collective nodes of all the nodes in its
        # collective operation.
        self.ready_collective_idxs: Set[Tuple[int, _DAGNodeOperationType]] = set()

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

        def compare(lhs: "_DAGOperationGraphNode", rhs: "_DAGOperationGraphNode"):
            # If both nodes belong to the same actor, the node with the smaller
            # `exec_task_idx` is prioritized. If two nodes belong to different
            # actors, it approximates balancing the scheduled tasks across actors,
            # by prioritizing the node with the smaller `exec_task_idx`. The tie
            # is broken by the `task_idx`.
            if lhs.operation.exec_task_idx != rhs.operation.exec_task_idx:
                return lhs.operation.exec_task_idx < rhs.operation.exec_task_idx
            return lhs.task_idx < rhs.task_idx

        if self.actor_handle == other.actor_handle:
            # When both nodes belong to the same actor, use the default comparison.
            return compare(self, other)
        elif self.is_nccl_op != other.is_nccl_op:
            # When one node is a NCCL operation and the other is not, prioritize
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
        If a node is not a NCCL collective, it is ready when it has a zero
        in-degree. If it is a NCCL collective, it is ready when all the nodes
        in its collective operation have zero in-degrees.
        """
        return self.in_degree == 0 and (
            len(self.ready_collective_idxs) == len(self.collective_idxs)
        )

    @property
    def is_read(self) -> bool:
        return self.operation.type == _DAGNodeOperationType.READ

    @property
    def is_nccl_collective(self) -> bool:
        """
        A node is a NCCL collective if it is a compute node and requires NCCL.
        """
        return (
            self.operation.type == _DAGNodeOperationType.COMPUTE and self.requires_nccl
        )

    @property
    def is_nccl_write(self) -> bool:
        """
        A node is a NCCL write if it is a write node and requires NCCL.
        """
        return self.operation.type == _DAGNodeOperationType.WRITE and self.requires_nccl

    @property
    def is_nccl_op(self) -> bool:
        return self.is_nccl_collective or self.is_nccl_write

    def viz_str(self):
        """
        A string representation of the node to be used in visualization.
        """
        return self.operation.viz_str()

    @property
    def _actor_id(self):
        return self.actor_handle._ray_actor_id.hex()


def _add_edge(
    from_node: _DAGOperationGraphNode,
    to_node: _DAGOperationGraphNode,
    label: str = "",
    control_dependency: bool = False,
):
    """
    Add an edge from `from_node` to `to_node`.

    Args:
        from_node: The node from which the edge originates.
        to_node: The node to which the edge points.
        label: The label of the edge. This will be used to annotate the edge
            in the visualization of the execution schedule.
    """
    from_node.out_edges[(to_node.task_idx, to_node.operation.type)] = (
        label,
        control_dependency,
    )
    to_node.in_edges[(from_node.task_idx, from_node.operation.type)] = (
        label,
        control_dependency,
    )


def _push_candidate_node_if_ready(
    actor_to_candidates: Dict["ray._raylet.ActorID", List[_DAGOperationGraphNode]],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
    node: _DAGOperationGraphNode,
) -> None:
    # Collective operations are ready when all the collective nodes have zero
    # in-degrees. Only one node per collective will be added as ready.
    if node.is_nccl_collective:
        for collective_node_metadata in node.collective_idxs:
            task_idx, op_type = collective_node_metadata
            collective_node = graph[task_idx][op_type]
            collective_node.ready_collective_idxs.add(
                (node.task_idx, node.operation.type)
            )
    if node.is_ready:
        heapq.heappush(
            actor_to_candidates[node.actor_handle._actor_id],
            node,
        )


def _select_next_nodes(
    actor_to_candidates: Dict["ray._raylet.ActorID", List[_DAGOperationGraphNode]],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
) -> Optional[List[_DAGOperationGraphNode]]:
    """
    This function selects the next nodes for the topological sort to generate
    execution schedule. If there are multiple candidate _DAGOperationGraphNodes,
    select the node with the top priority. The priority is defined in
    `_DAGOperationGraphNode.__lt__`.

    For the implementation details, we maintain a priority queue for each actor,
    where the head of the priority queue is the node with the smallest `exec_task_idx`.
    When a node has a zero in-degree, it is added to the corresponding actor's
    priority queue. For a node other than a NCCL collective node, it is ready to be
    executed if it has a zero in-degree. For a NCCL collective node, it is ready to
    be executed when all the nodes in its collective operation have zero in-degrees.

    If a node is a NCCL collective node, it updates the `ready_collective_nodes` of
    all the nodes in its collective operation. Unless all the nodes in its collective
    group have zero in-degrees, this node is removed from the candidate list.
    Eventually, exactly one NCCL collective node from its collective operation is
    selected from the candidate list.

    If the selected node is a NCCL write node, select all the downstream NCCL
    read nodes. If the selected node is a NCCL collective node, select all the NCCL
    compute nodes in its collective operation.

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
        # a NCCL write node is picked. NCCL is a blocking operation, so we need
        # to pick all the corresponding NCCL read nodes to avoid a deadlock.
        for downstream_node_metadata in top_priority_node.out_edges:
            task_idx, op_type = downstream_node_metadata
            downstream_node = graph[task_idx][op_type]
            assert downstream_node.is_read
            next_nodes.append(downstream_node)
        assert len(next_nodes) == 1 + len(top_priority_node.out_edges)
    elif top_priority_node.is_nccl_collective:
        # a NCCL collective node is picked. NCCL is a blocking operation, so we need
        # to pick all the corresponding NCCL collective nodes in its collective
        # operation to avoid a deadlock.
        for collective_node_metadata in top_priority_node.collective_idxs:
            task_idx, op_type = collective_node_metadata
            collective_node = graph[task_idx][op_type]
            assert collective_node.is_nccl_collective and collective_node.is_ready
            if collective_node != top_priority_node:
                next_nodes.append(collective_node)
        assert len(next_nodes) == len(top_priority_node.collective_idxs)

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
                _add_edge(prev_compute_node, compute_node, "", True)
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
            # Class method output node dependencies are handled at its upstream:
            # i.e., class method node
            continue
        for downstream_task_idx in task.downstream_task_idxs:
            downstream_dag_node = idx_to_task[downstream_task_idx].dag_node
            if isinstance(downstream_dag_node, MultiOutputNode):
                continue
            if (
                isinstance(downstream_dag_node, ClassMethodNode)
                and downstream_dag_node.is_class_method_output
            ):
                consumer_idxs = idx_to_task[downstream_task_idx].downstream_task_idxs
                for consumer_idx in consumer_idxs:
                    if consumer_idx in graph:
                        _add_edge(
                            graph[task_idx][_DAGNodeOperationType.WRITE],
                            graph[consumer_idx][_DAGNodeOperationType.READ],
                            "nccl"
                            if graph[task_idx][
                                _DAGNodeOperationType.WRITE
                            ].requires_nccl
                            else "shm",
                        )
                continue
            _add_edge(
                graph[task_idx][_DAGNodeOperationType.WRITE],
                graph[downstream_task_idx][_DAGNodeOperationType.READ],
                "nccl"
                if graph[task_idx][_DAGNodeOperationType.WRITE].requires_nccl
                else "shm",
            )

    return graph


def _actor_viz_label(actor: "ray.actor.ActorHandle"):
    """
    Returns the label of an actor in the visualization of the execution schedule.

    Args:
        actor: The actor to be represented.
    """
    class_name = actor._ray_actor_creation_function_descriptor.class_name
    actor_id = actor._ray_actor_id.hex()
    return f"Actor class name: {class_name}\nActor ID: {actor_id}"


def _node_viz_id_and_label(
    node: _DAGOperationGraphNode, idx: int, optimized_index: int
):
    """
    Returns the visualization id and label of a node. The visualization id is unique
    across all nodes.

    Args:
        node: The node to be represented.
        idx: The index of the node in the execution schedule.
        optimized_index: The index of the node in the optimized execution schedule.
    """
    node_viz_label = node.viz_str() + f" {idx},{optimized_index}"
    node_viz_id = f"{node._actor_id}_{node_viz_label}"
    return node_viz_id, node_viz_label


def _visualize_execution_schedule(
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ],
    actor_to_overlapped_schedule: Optional[
        Dict["ray.actor.ActorHandle", List[_DAGOperationGraphNode]]
    ],
    graph: Dict[int, Dict[_DAGNodeOperationType, _DAGOperationGraphNode]],
):
    """
    Visualize the execution schedule for each actor.

    The visualization will be saved as a PNG file named `compiled_graph_schedule.png`.
    Details of the visualization: # noqa

        Node description format:
            [<task_index>] <method_name> <operation> <orig_index>, <overlap_index>

        Node description fields:
            operation: is R(READ), C(COMPUTE), or W(WRITE)
            orig_index: the index in the original execution schedule
            overlap_index: the index in the overlap-communication optimized execution schedule
            If this is different from orig_index, the node is highlighted in red color

        Node grouping:
            The nodes belonging to the same actor are grouped in the same rectangle
            The actor class name and the actor id are shown in the rectangle

        Edges:
            black color (without label): data dependency
            black color (annotated with "shm"): shared memory channel
            blue color (annotated with "nccl): NCCL channel
            dashed edge: control dependency between compute operations

    Args:
        actor_to_execution_schedule: A dictionary that maps an actor handle to
            the execution schedule which is a list of operation nodes.
        actor_to_overlapped_schedule: A dictionary that maps an actor handle to the
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
    # A dictionary that maps a node to its visualization id
    node_to_viz_id: Dict[_DAGOperationGraphNode, str] = {}

    if actor_to_overlapped_schedule is None:
        # TODO(rui): make the visualization more concise by only displaying
        # the original schedule
        actor_to_overlapped_schedule = actor_to_execution_schedule
    for actor, execution_nodes in actor_to_execution_schedule.items():
        overlapped_schedule = actor_to_overlapped_schedule[actor]
        node_to_optimized_index = {
            node: i for i, node in enumerate(overlapped_schedule)
        }

        actor_id = actor._ray_actor_id.hex()
        with dot.subgraph(name=f"cluster_{actor_id}") as subgraph:
            subgraph.attr(rank=actor_id, label=_actor_viz_label(actor))
            for i, node in enumerate(execution_nodes):
                optimized_index = node_to_optimized_index.get(node)
                node_viz_id, node_viz_label = _node_viz_id_and_label(
                    node, i, optimized_index
                )
                color = "red" if optimized_index != i else "black"
                subgraph.node(node_viz_id, node_viz_label, color=color)
                node_to_viz_id[node] = node_viz_id

    for actor, execution_nodes in actor_to_execution_schedule.items():
        for i, node in enumerate(execution_nodes):
            node_viz_id = node_to_viz_id[node]
            for out_edge, viz_info in node.out_edges.items():
                label, control_dependency = viz_info
                out_task_idx, out_op_type = out_edge
                out_node = graph[out_task_idx][out_op_type]
                out_node_viz_id = node_to_viz_id[out_node]
                color = "blue" if label == "nccl" else "black"
                style = "dashed" if control_dependency else "solid"
                dot.edge(
                    node_viz_id, out_node_viz_id, label=label, color=color, style=style
                )

    # Add legend
    with dot.subgraph(name="cluster_legend") as legend:
        legend.attr(label="Legend", labelloc="t", fontsize="20", bgcolor="lightgrey")

        # Single node and its explanation
        legend.node("example_node", "[0] bwd C 10,10\n")
        explanation = (
            '<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">'  # noqa
            '<TR><TD ALIGN="LEFT"><B>Node description format:</B></TD></TR>'
            '<TR><TD ALIGN="LEFT">[&lt;task_index&gt;] &lt;method_name&gt; &lt;operation&gt; &lt;orig_index&gt;, &lt;overlap_index&gt;</TD></TR>'  # noqa
            "<TR><TD></TD></TR>"
            '<TR><TD ALIGN="LEFT"><B>Node description fields:</B></TD></TR>'
            '<TR><TD ALIGN="LEFT">operation: is R(READ), C(COMPUTE), or W(WRITE)</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT">orig_index: the index in the original execution schedule</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT">overlap_index: the index in the overlap-communication optimized execution schedule</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT">If this is different from orig_index, the node is highlighted in <FONT COLOR="red">red color</FONT></TD></TR>'  # noqa
            "<TR><TD></TD></TR>"
            '<TR><TD ALIGN="LEFT"><B>Node grouping:</B></TD></TR>'
            '<TR><TD ALIGN="LEFT">The nodes belonging to the same actor are grouped in the same rectangle</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT">The actor class name and the actor id are shown in the rectangle</TD></TR>'  # noqa
            "<TR><TD></TD></TR>"
            '<TR><TD ALIGN="LEFT"><B>Edges:</B></TD></TR>'
            '<TR><TD ALIGN="LEFT">black color (without label): data dependency</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT">black color (annotated with "shm"): shared memory channel</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT"><FONT COLOR="blue">blue color</FONT> (annotated with "nccl): NCCL channel</TD></TR>'  # noqa
            '<TR><TD ALIGN="LEFT">dashed edge: control dependency between compute operations</TD></TR>'  # noqa
            "</TABLE>>"
        )

        legend.node("example_explanation", explanation, shape="plaintext")
        legend.edge("example_node", "example_explanation", style="invis")

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
                _push_candidate_node_if_ready(actor_to_candidates, graph, node)

    visited_nodes = set()

    # Use topological sort algorithm to generate the execution schedule.
    while True:
        # Select a list of nodes to be executed. There are three cases:
        # 1. If a selected node is not a NCCL operation, only itself is returned.
        # 2. If a selected node is a NCCL write operation, the corresponding NCCL
        #    read operations are also returned.
        # 3. If a selected node is a NCCL collective operation, all the nodes in
        #    its collective operation are returned.
        # In cases 1 and 3, all the selected nodes are ready. In case 2, the NCCL
        # write node is ready, while the NCCL read nodes are not ready until their
        # in-degrees are updated.
        nodes = _select_next_nodes(actor_to_candidates, graph)
        if nodes is None:
            break
        # Filter out the visited nodes.
        nodes = [node for node in nodes if node not in visited_nodes]
        # Add the selected nodes to the execution schedule.
        for node in nodes:
            actor_to_execution_schedule[node.actor_handle].append(node)
            visited_nodes.add(node)
        # Update the in-degree of the downstream nodes.
        for node in nodes:
            for out_node_task_idx, out_node_type in node.out_edges:
                out_node = graph[out_node_task_idx][out_node_type]
                out_node.in_edges.pop((node.task_idx, node.operation.type))
                if out_node.in_degree == 0 and out_node not in visited_nodes:
                    # If the downstream node is already visited, it has been added
                    # to the execution schedule. They are the NCCL read nodes in
                    # case 2.
                    _push_candidate_node_if_ready(actor_to_candidates, graph, out_node)
    assert len(visited_nodes) == len(graph) * 3, "Expected all nodes to be visited"
    for node in visited_nodes:
        assert node.is_ready, f"Expected {node} to be ready"
    for _, candidates in actor_to_candidates.items():
        assert len(candidates) == 0, "Expected all candidates to be empty"

    return actor_to_execution_schedule


def _generate_overlapped_execution_schedule(
    actor_to_execution_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ],
) -> Dict["ray.actor.ActorHandle", List[_DAGOperationGraphNode]]:
    """
    From an existing execution schedule, generate a new schedule by overlapping
    computation and communication.

    Currently, the algorithm generates a new schedule for each actor as follows:
    For each NCCL read operation (i.e., recv), scan backwards to find the nearest
    compute node to swap with so that the NCCL read operation can be overlapped
    with computation.

    Collective operations are not yet supported.

    Args:
        actor_to_execution_schedule: A dictionary that maps an actor handle to
            the existing execution schedule for the actor. The schedule is a list
            is a list of operations to be executed.

    Returns:
        A dictionary that maps an actor handle to the overlapped execution schedule
        for the actor.
    """

    actor_to_overlapped_schedule: Dict[
        "ray.actor.ActorHandle", List[_DAGOperationGraphNode]
    ] = copy.deepcopy(actor_to_execution_schedule)
    for overlapped_schedule in actor_to_overlapped_schedule.values():
        for i in range(len(overlapped_schedule)):
            if (
                overlapped_schedule[i].operation.type == _DAGNodeOperationType.READ
                and overlapped_schedule[i].requires_nccl
            ):
                # For each NCCL read operation (i.e., recv), scan backwards
                # to find the nearest compute node to swap with so that
                # the NCCL read operation can be overlapped with computation.
                for j in range(i - 1, -1, -1):
                    if (
                        overlapped_schedule[j].operation.type
                        == _DAGNodeOperationType.COMPUTE
                    ):
                        # Found a desired compute operation, make the swap
                        nccl_read_op = overlapped_schedule[i]
                        prev_ops = overlapped_schedule[j:i]
                        overlapped_schedule[j + 1 : i + 1] = prev_ops
                        overlapped_schedule[j] = nccl_read_op
                        break
                    if (
                        overlapped_schedule[j].operation.type
                        == _DAGNodeOperationType.READ
                        or overlapped_schedule[j].operation.type
                        == _DAGNodeOperationType.WRITE
                    ) and overlapped_schedule[j].requires_nccl:
                        # Found a NCCL read/write operation, skip the overlap
                        # optimization to keep relative order of NCCL operations
                        break
    return actor_to_overlapped_schedule


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
