from dataclasses import dataclass
from typing import List

from networkx import DiGraph


@dataclass
class Dep:
    """Represents a dependency with its name, version, and dependents."""
    name: str
    version: str
    required_by: List[str]


class DependencyGraph:
    def __init__(self):
        self.graph = DiGraph()

    def add_nodes(self, deps: List[Dep]):
        for dep in deps:
            if not self.graph.has_node(dep.name):
                self.graph.add_node(dep.name, version=dep.version)

    def add_edges(self, deps: List[Dep]):
        for dep in deps:
            if len(dep.required_by) == 0:
                continue
            for dependent in dep.required_by:
                if not self.graph.has_node(dependent):
                    raise ValueError(f"Dependency {dependent} not found in deps")
                # Edge: dependent -> dep (the package that requires points to what it requires)
                self.graph.add_edge(dependent, dep.name)

    def build_dependency_graph(self, deps: List[Dep]):
        self.add_nodes(deps)
        self.add_edges(deps)

    def remove_dropped_dependencies(self, dropped_dependencies: List[str]):
        for dep in dropped_dependencies:
            if not self.graph.has_node(dep):
                raise ValueError(f"Dependency {dep} not found in deps")

        to_remove = set(dropped_dependencies)

        # Cascade: find nodes that should be removed because nothing else requires them
        # A node should be removed if ALL its predecessors (dependents) are being removed
        changed = True
        while changed:
            changed = False
            for node in list(self.graph.nodes()):
                if node in to_remove:
                    continue
                # Get all packages that require this node (predecessors point TO this node)
                dependents = set(self.graph.predecessors(node))
                # If this node has dependents and ALL of them are being removed,
                # then this node is no longer needed
                if dependents and dependents.issubset(to_remove):
                    to_remove.add(node)
                    changed = True

        self.graph.remove_nodes_from(to_remove)
