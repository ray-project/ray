from typing import List

from networkx import DiGraph

from ci.raydepsets.parser import Dep


class DependencyGraph:
    def __init__(self):
        self.graph = DiGraph()

    # def add_node(self, dep: Dep):
    #     # get root nodes (no deps in the list)
    #     self.graph.add_node(dep.name, version=dep.version)
    #     for dependency in dep.dependencies:
    #         if not self.graph.has_node(dependency):
    #             self.add_node(Dep(name=dependency, version="", dependencies=[]))
    #         self.graph.add_edge(dep.name, dependency)

    def build_dependency_graph(self, deps: List[Dep]):
        for dep in deps:
            if not self.graph.has_node(dep.name):
                self.graph.add_node(dep.name, version=dep.version)
            for dependency in dep.dependencies:
                if not self.graph.has_node(dependency):
                    self.graph.add_node(dependency, version="")
                    self.graph.add_edge(dep.name, dependency)

        # get root nodes (no deps in the list)
        root_deps = [dep for dep in deps if not dep.dependencies]
        for dep in root_deps:
            self.graph.add_node(dep.name, version=dep.version)
            # create child nodes
            for dependency in dep.dependencies:
                if not self.graph.has_node(dependency):
                    # get child node from deps
                    child_dep = next((d for d in deps if d.name == dependency), None)
                    if not child_dep:
                        raise ValueError(f"Dependency {dependency} not found in deps")
                    self.graph.add_node(child_dep.name, version=child_dep.version)
                    self.graph.add_edge(dep.name, child_dep.name)
