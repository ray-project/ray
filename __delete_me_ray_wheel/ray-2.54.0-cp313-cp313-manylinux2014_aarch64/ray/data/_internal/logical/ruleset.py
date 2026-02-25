import collections
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Type

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
        then this method yields A before B. The order is otherwise undefined.
        """
        roots = self._build_graph()
        queue = collections.deque(roots)
        while queue:
            node = queue.popleft()
            yield node.rule
            queue.extend(node.dependents)

    def _build_graph(self) -> List["Ruleset._Node"]:
        # NOTE: Because the number of rules will always be relatively small, I've opted
        # for a simpler but inefficient implementation.

        # Step 1: Add edges from dependencies to their dependants.
        rule_to_node: Dict[Type[Rule], "Ruleset._Node"] = {
            rule: Ruleset._Node(rule) for rule in self._rules
        }
        for rule in self._rules:
            node = rule_to_node[rule]

            # These are rules that must be applied *before* this rule.
            for dependency in rule.dependencies():
                if dependency in rule_to_node:
                    rule_to_node[dependency].dependents.append(node)

            # These are rules that must be applied *after* this rule.
            for dependent in rule.dependents():
                if dependent in rule_to_node:
                    node.dependents.append(rule_to_node[dependent])

        # Step 2: Determine which nodes are roots.
        roots = list(rule_to_node.values())
        for node in rule_to_node.values():
            for dependent in node.dependents:
                if dependent in roots:
                    roots.remove(dependent)

        return roots

    def _contains_cycle(self) -> bool:
        if not self._rules:
            return

        # If the graph contains nodes but there aren't any root nodes, it means that
        # there must be a cycle.
        roots = self._build_graph()
        return not roots
