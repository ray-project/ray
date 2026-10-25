import collections
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Set, Tuple, Type

from ray.data._internal.logical.interfaces import Rule
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class Ruleset:
    """A collection of rules to apply to a plan.

    This is a utility class to ensure that, if rules depend on each other, they're
    applied in a correct order.
    """

    @dataclass(frozen=True)
    class _Node:
        rule: Type[Rule]
        dependents: List["Ruleset._Node"] = field(default_factory=list)

    def __init__(self, rules: Optional[List[Type[Rule]]] = None):
        if rules is None:
            rules = []

        self._rules = list(rules)

    def add(self, rule: Type[Rule]):
        if rule in self._rules:
            raise ValueError(f"Rule {rule} already in ruleset")

        self._rules.append(rule)

        if self._contains_cycle():
            raise ValueError("Cannot add rule that would create a cycle")

    def remove(self, rule: Type[Rule]):
        if rule not in self._rules:
            raise ValueError(f"Rule {rule} not found in ruleset")

        self._rules.remove(rule)

    def __iter__(self) -> Iterator[Type[Rule]]:
        """Iterate over the rules in this ruleset.

        This method yields rules in dependency order. For example, if B depends on A,
        then this method yields A before B. Each rule is yielded exactly once, and a
        rule is only yielded once *all* of its dependencies have been yielded (so a
        rule that several others must precede is not emitted early or duplicated).
        Insertion order breaks ties among rules that are ready at the same time.
        """
        order, _ = self._topological_order()
        for node in order:
            yield node.rule

    def _topological_order(self) -> Tuple[List["Ruleset._Node"], int]:
        """Order the nodes by dependency using Kahn's algorithm.

        Returns the topologically-ordered nodes and the total node count.
        A node is enqueued the moment its in-degree (count of not-yet-emitted
        dependencies) hits zero; since an in-degree only decreases and we
        enqueue solely on the zero-crossing, each node is emitted exactly once.
        Insertion order breaks ties among nodes that are ready together.

        Nodes that participate in a cycle never reach in-degree zero, so they
        are absent from the result -- i.e. ``len(order) < total`` exactly when
        the graph contains a cycle.
        """
        nodes, indegree = self._build_graph()
        queue = collections.deque(n for n in nodes if indegree[id(n)] == 0)
        order: List["Ruleset._Node"] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for dep in node.dependents:
                indegree[id(dep)] -= 1
                if indegree[id(dep)] == 0:
                    queue.append(dep)
        return order, len(nodes)

    def _build_graph(
        self,
    ) -> Tuple[List["Ruleset._Node"], Dict[int, int]]:
        """Build the dependency DAG.

        Returns the nodes (one per rule, in insertion order) and their
        in-degrees -- the number of rules that must be applied before each.
        The in-degree map is keyed by node identity (``id``) rather than rule
        type so that distinct nodes never share a counter, and is computed as
        the edges are added (every incoming edge bumps the target's in-degree)
        rather than re-derived by a second traversal. A node whose in-degree
        is zero is a root.
        """
        rule_to_node: Dict[Type[Rule], "Ruleset._Node"] = {
            rule: Ruleset._Node(rule) for rule in self._rules
        }
        indegree: Dict[int, int] = {id(node): 0 for node in rule_to_node.values()}

        # De-duplicate edges. The same ordering can be declared from both ends
        # -- rule A lists B in ``dependencies()`` while B lists A in
        # ``dependents()`` -- which would otherwise add the edge twice,
        # double-counting the in-degree and duplicating ``dependents`` entries.
        seen_edges: Set[Tuple[int, int]] = set()

        def add_edge(before: "Ruleset._Node", after: "Ruleset._Node") -> None:
            """Record that ``before`` must be applied before ``after``."""
            edge = (id(before), id(after))
            if edge in seen_edges:
                return
            seen_edges.add(edge)
            before.dependents.append(after)
            indegree[id(after)] += 1

        for rule in self._rules:
            node = rule_to_node[rule]

            # Rules that must be applied *before* this rule: dependency -> node.
            for dependency in rule.dependencies():
                if dependency in rule_to_node:
                    add_edge(rule_to_node[dependency], node)

            # Rules that must be applied *after* this rule: node -> dependent.
            for dependent in rule.dependents():
                if dependent in rule_to_node:
                    add_edge(node, rule_to_node[dependent])

        return list(rule_to_node.values()), indegree

    def _contains_cycle(self) -> bool:
        # Kahn's traversal drops any node stuck in a cycle (its in-degree never
        # reaches zero), so a shortfall between the ordered nodes and the total
        # means a cycle exists. This correctly flags a graph that mixes an
        # acyclic component with a disjoint cycle -- a plain "does any root
        # exist?" check would be fooled by the acyclic root.
        order, total = self._topological_order()
        return len(order) != total
