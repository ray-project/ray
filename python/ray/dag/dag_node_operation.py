from functools import total_ordering
from enum import Enum
from ray.util.annotations import DeveloperAPI


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
    def __init__(self, operation: DAGNodeOperation, idx, dag_node):
        self.operation = operation
        self.idx = idx
        from ray.dag import ClassMethodNode

        assert isinstance(dag_node, ClassMethodNode)
        self.actor_handle = dag_node._get_actor_handle()
        self.requires_nccl = dag_node.type_hint.requires_nccl()
        self.in_edges = set()
        self.out_edges = set()

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
        if self.operation.idx == other.operation.idx:
            return self.operation.type == other.operation.type
        return False

    def __hash__(self):
        return hash((self.operation, self.idx))

    def add_edge(self, out_node: "DAGOperationGraphNode"):
        self.out_edges.add((out_node.idx, out_node.operation.type))
        out_node.in_edges.add((self.idx, self.operation.type))
