from typing import List

from networkx import DiGraph, isolates

from ci.raydepsets.parser import Dep


class DependencyGraph:
    def __init__(self):
        self.graph = DiGraph()

    def add_nodes(self, deps: List[Dep]):
        for dep in deps:
            if not self.graph.has_node(dep.name):
                self.graph.add_node(dep.name, version=dep.version)

    def add_edges(self, deps: List[Dep]):
        for dep in deps:
            for dependency in dep.required_by:
                if not self.graph.has_node(dependency):
                    raise ValueError(f"Dependency {dependency} not found in deps")
                self.graph.add_edge(dep.name, dependency)

    def build_dependency_graph(self, deps: List[Dep]):
        self.add_nodes(deps)
        self.add_edges(deps)

    def remove_dropped_dependencies(self, dropped_dependencies: List[str]):
        for dep in dropped_dependencies:
            if not self.graph.has_node(dep):
                raise ValueError(f"Dependency {dep} not found in deps")
            self.graph.remove_node(dep)
            for dependency in self.graph.successors(dep):
                if dependency not in dropped_dependencies:
                    self.graph.remove_edge(dep, dependency)
        self.remove_orphan_nodes()

    def remove_orphan_nodes(self) -> List[str]:
        """Remove nodes that have no incoming or outgoing edges."""
        orphans = list(isolates(self.graph))
        self.graph.remove_nodes_from(orphans)
        return orphans
