from typing import TYPE_CHECKING, List, Sequence, Set, cast

from ray.data.preprocessor import Preprocessor

if TYPE_CHECKING:
    from ray.data.aggregate import AggregateFnV2
    from ray.data.preprocessors.utils import AggregateStatSpec


class _DAGNode:
    """Base class for nodes in the deferred-fit aggregation DAG.

    Each node tracks column-level dependencies: a node depends on an earlier
    one iff it reads any column the earlier one writes. This enables
    topological scheduling of aggregations based on column lineage - members
    that touch disjoint columns can have their statistics computed in the same
    dataset scan.

    A member that declares no input or output columns may read and write any
    column, so its node is treated as reading and writing every column: it
    depends on every earlier node, and every later node depends on it.

    Args:
        preprocessor: The preprocessor this node belongs to.
    """

    def __init__(self, preprocessor: Preprocessor):
        self.preprocessor: Preprocessor = preprocessor
        self.dependencies: Set["_DAGNode"] = set()
        self.dependents: Set["_DAGNode"] = set()
        self.completed: bool = False
        self.read_cols: Set[str] = set(preprocessor.get_input_columns())
        self.write_cols: Set[str] = set(preprocessor.get_output_columns())
        self.touches_all_columns: bool = not _declares_columns(preprocessor)

    def depends_on(self, earlier: "_DAGNode") -> bool:
        """Whether this node must wait for an earlier chain member's node."""
        return (
            self.touches_all_columns
            or earlier.touches_all_columns
            or bool(self.read_cols & earlier.write_cols)
        )

    @property
    def is_placeholder(self) -> bool:
        return isinstance(self, _PlaceholderNode)


class _AggregationNode(_DAGNode):
    """Node representing a single aggregation of one preprocessor.

    Args:
        preprocessor: The preprocessor this aggregation belongs to.
        spec: The :class:`~ray.data.preprocessors.utils.AggregateStatSpec`
            registered on the preprocessor's stat computation plan.
    """

    def __init__(self, preprocessor: Preprocessor, spec: "AggregateStatSpec"):
        super().__init__(preprocessor)
        self.spec: "AggregateStatSpec" = spec

    @property
    def agg_fn(self) -> "AggregateFnV2":
        # NOTE: A plan's aggregate specs always hold an aggregation (callable
        #       stats are separate specs), but the spec type is a broader union.
        return cast("AggregateFnV2", self.spec.stat_fn)


class _PlaceholderNode(_DAGNode):
    """Placeholder node for members that contribute no aggregations.

    Covers non-fittable preprocessors and fittable members whose plan
    registered nothing (e.g. a nested Chain, which computes its own statistics
    when transformed). These nodes have nothing to compute, but they
    participate in dependency tracking so that transforms are ordered
    correctly.
    """


def _declares_columns(preprocessor: Preprocessor) -> bool:
    """Whether a preprocessor declares the columns it reads or writes.

    A Chain declares its columns only if every member does.
    """
    from ray.data.preprocessors.chain import Chain

    if isinstance(preprocessor, Chain):
        return all(_declares_columns(p) for p in preprocessor.preprocessors)
    return bool(preprocessor.get_input_columns() or preprocessor.get_output_columns())


def _build_aggregation_dag(
    preprocessors: Sequence[Preprocessor],
) -> List[_DAGNode]:
    """Construct the aggregation DAG for a chain of preprocessors.

    Each fittable member contributes one node per aggregation registered on
    its plan; every other member contributes a placeholder node. A later node
    depends on an earlier one iff it reads a column the earlier one writes, or
    either of them doesn't declare its columns.

    Args:
        preprocessors: The chain's members, in chain order.

    Returns:
        All nodes, in chain order (a member's aggregation nodes are adjacent).
    """
    all_nodes: List[_DAGNode] = []
    nodes_per_preprocessor: List[List[_DAGNode]] = []

    for preprocessor in preprocessors:
        nodes: List[_DAGNode] = []
        if preprocessor._is_fittable:
            # NOTE: Every registered aggregation is pending, even when `stats_`
            #       is already partly filled (e.g. `Categorizer` copies the
            #       dtypes it was given before registering the rest).
            for agg_spec in preprocessor._stat_computation_plan:
                nodes.append(_AggregationNode(preprocessor=preprocessor, spec=agg_spec))
        if not nodes:
            nodes.append(_PlaceholderNode(preprocessor=preprocessor))

        nodes_per_preprocessor.append(nodes)
        all_nodes.extend(nodes)

    # Add edges based on read/write column overlap between chain positions.
    for i in range(len(preprocessors)):
        for j in range(i):
            for cur_node in nodes_per_preprocessor[i]:
                for prev_node in nodes_per_preprocessor[j]:
                    if cur_node.depends_on(prev_node):
                        cur_node.dependencies.add(prev_node)
                        prev_node.dependents.add(cur_node)

    _validate_dag_is_acyclic(all_nodes)
    return all_nodes


def _validate_dag_is_acyclic(nodes: List[_DAGNode]) -> None:
    visited = set()
    stack = set()

    def visit(node: _DAGNode):
        if node in stack:
            raise RuntimeError("Cycle detected in aggregation DAG")
        if node in visited:
            return
        stack.add(node)
        for dep in node.dependencies:
            visit(dep)
        stack.remove(node)
        visited.add(node)

    for node in nodes:
        visit(node)
