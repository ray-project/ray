from typing import TYPE_CHECKING, List, Sequence, Set

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

    def is_ready(self) -> bool:
        """Returns True if every dependency has completed."""
        return all(dep.completed for dep in self.dependencies)

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
        return self.spec.stat_fn


class _PlaceholderNode(_DAGNode):
    """Placeholder node for members that contribute no aggregations.

    Covers non-fittable preprocessors, fittable members that already have
    stats (e.g. fitted eagerly), and fittable members whose plan registered
    nothing. These nodes have nothing to compute, but they participate in
    dependency tracking so that transforms are ordered correctly: a
    placeholder completes only when its own dependencies complete, which keeps
    any member that reads this preprocessor's output columns from aggregating
    before this preprocessor's transform has been applied.
    """


def _build_aggregation_dag(
    preprocessors: Sequence[Preprocessor],
) -> List[_DAGNode]:
    """Construct the aggregation DAG for a chain of preprocessors.

    Each fittable member contributes one node per registered aggregation;
    every other member contributes a placeholder node. Edges follow one rule:
    a later node depends on an earlier one iff it reads any column the earlier
    one writes.

    Args:
        preprocessors: The chain's members, in chain order.

    Returns:
        All nodes, in chain order (a member's aggregation nodes are adjacent).
    """
    all_nodes: List[_DAGNode] = []
    nodes_per_preprocessor: List[List[_DAGNode]] = []

    for preprocessor in preprocessors:
        nodes: List[_DAGNode] = []
        if preprocessor._is_fittable and not preprocessor.has_stats():
            for agg_spec in preprocessor._stat_computation_plan:
                nodes.append(_AggregationNode(preprocessor=preprocessor, spec=agg_spec))
        if not nodes:
            # Non-fittable, already fitted, or nothing registered on the plan
            # (e.g. a nested Chain, which runs its own DAG when transformed).
            nodes.append(_PlaceholderNode(preprocessor=preprocessor))

        nodes_per_preprocessor.append(nodes)
        all_nodes.extend(nodes)

    # Add edges based on read/write column overlap between chain positions.
    for i in range(len(preprocessors)):
        for j in range(i):
            for cur_node in nodes_per_preprocessor[i]:
                for prev_node in nodes_per_preprocessor[j]:
                    if cur_node.read_cols & prev_node.write_cols:
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
