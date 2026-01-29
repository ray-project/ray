from dataclasses import dataclass
from typing import List

from networkx import DiGraph, descendants


@dataclass
class Dep:
    """Represents a dependency with its name, version, and dependents."""

    name: str
    version: str
    required_by: List[str]


class DependencyGraph:
    """Manages a dependency graph of Python packages."""

    def __init__(self):
        self.graph = DiGraph()

    def add_nodes(self, deps: List[Dep]):
        for dep in deps:
            if not self.graph.has_node(dep.name):
                self.graph.add_node(dep.name, version=dep.version)

    def add_edges(self, deps: List[Dep]):
        for dep in deps:
            for dependent in dep.required_by:
                if not self.graph.has_node(dependent):
                    raise ValueError(f"Dependency {dependent} not found in deps")
                self.graph.add_edge(dependent, dep.name)

    def build_dependency_graph(self, deps: List[Dep]):
        self.add_nodes(deps)
        self.add_edges(deps)

    def remove_dropped_dependencies(self, dropped_dependencies: List[str]):
        """Removes a list of dependencies and any other dependencies that become
        orphaned as a result.

        A dependency is considered orphaned if all packages that require it are
        also removed.

        Args:
            dropped_dependencies: A list of dependency names to remove.

        Raises:
            ValueError: If a dependency in `dropped_dependencies` is not in the graph.
        """
        for dep in dropped_dependencies:
            if not self.graph.has_node(dep):
                raise ValueError(f"Dependency {dep} not found in deps")

        to_remove = set(dropped_dependencies)

        # Only descendants of dropped packages can become orphaned
        candidates = set()
        for dep in dropped_dependencies:
            candidates.update(descendants(self.graph, dep))

        # Check each candidate - remove if ALL predecessors are being removed
        changed = True
        while changed:
            changed = False
            for node in candidates - to_remove:
                dependents = set(self.graph.predecessors(node))
                if dependents and dependents.issubset(to_remove):
                    to_remove.add(node)
                    changed = True

        self.graph.remove_nodes_from(to_remove)
