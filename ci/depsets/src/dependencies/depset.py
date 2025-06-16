from typing import List, Dict, Optional
from collections import defaultdict
import os

class Dep:
    def __init__(self, name: str, constraints: List[str], version: str):
        self.name = name
        self.version = version
        self.constraints = constraints

    def __str__(self):
        if self.constraints and self.version:
            return f"{self.name}{self.constraints[0]}{self.version}"
        return self.name

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_requirement(cls, requirement: str) -> "Dep":
        # Simple parsing of requirement strings like "package==1.0.0" or "package>=1.0.0"
        parts = requirement.split("==")
        if len(parts) == 2:
            return cls(parts[0], ["=="], parts[1])
        parts = requirement.split(">=")
        if len(parts) == 2:
            return cls(parts[0], [">="], parts[1])
        parts = requirement.split("<=")
        if len(parts) == 2:
            return cls(parts[0], ["<="], parts[1])
        return cls(requirement, [], "")

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "constraints": self.constraints,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "Dep":
        return cls(
            data["name"],
            data["constraints"],
            data["version"]
        )

class DepSet:
    def __init__(self, requirements_fp: str):
        self.requirements_fp = requirements_fp
        self.dependencies: List[Dep] = []
        self._load_requirements()

    def _load_requirements(self):
        if not os.path.exists(self.requirements_fp):
            return
        with open(self.requirements_fp) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    self.dependencies.append(Dep.from_requirement(line))

    def compile(self, constraints: List[str], set: Optional["DepSet"] = None):
        # Creates a depset from a list of constraint files or an existing depset
        pass

    def subset(self, packages: List[str]) -> "DepSet":
        # Creates a depset based on existing set and a set of package names (min dep set)
        pass

    def expand(self, deps: List[Dep], constraints: List[str]) -> "DepSet":
        # Creates a new expanded depset based on 1 or more depsets and 1 or more constraint files
        pass

    def relax(self, degree: int) -> "DepSet":
        # Converts a set back into versioned constraints - keeping select dep versions pinned
        pass

    def to_dict(self) -> Dict:
        return {
            "requirements_fp": self.requirements_fp,
            "dependencies": [dep.to_dict() for dep in self.dependencies]
        }

    def to_txt(self) -> str:
        return "\n".join([str(dep) for dep in self.dependencies])

    @classmethod
    def from_dict(cls, data: Dict) -> "DepSet":
        depset = cls(data["requirements_fp"])
        depset.dependencies = [Dep.from_dict(dep_data) for dep_data in data["dependencies"]]
        return depset

class DepGraph:
    def __init__(self, deps: List[Dep]):
        self.deps = deps
        self.graph: Dict[str, Dep] = {}
        for dep in deps:
            self.add_dep(dep)

    def add_dep(self, dep: Dep):
        self.graph[dep.name] = dep

class DependencyDAG:
    def __init__(self):
        self.adj_list = defaultdict(set)      # parent -> set of children
        self.reverse_adj = defaultdict(set)   # child -> set of parents

    def add_dep(self, dep):
        self.adj_list[dep]  # Ensures the node is in the graph
        self.reverse_adj[dep]

    def add_edge(self, parent, child):
        self.add_dep(parent)
        self.add_dep(child)
        self.adj_list[parent].add(child)
        self.reverse_adj[child].add(parent)

    def get_nodes(self):
        return set(self.adj_list.keys())

    def get_edges(self):
        return [(src, dst) for src in self.adj_list for dst in self.adj_list[src]]

    def get_children(self, dep):
        return self.adj_list.get(dep, set())

    def get_parents(self, dep):
        return self.reverse_adj.get(dep, set())

    def get_root_nodes(self):
        # Nodes with no parents (in-degree = 0)
        return {dep for dep in self.adj_list if not self.reverse_adj[dep]}

    def detect_cycles(self):
        visited = set()
        rec_stack = set()

        def dfs(node):
            if node in rec_stack:
                return True  # Cycle detected
            if node in visited:
                return False
            visited.add(node)
            rec_stack.add(node)
            for neighbor in self.adj_list[node]:
                if dfs(neighbor):
                    return True
            rec_stack.remove(node)
            return False

        return any(dfs(node) for node in self.adj_list)

    def get_descendants(self, start_node, max_depth=1):
        """Return all downstream nodes within max_depth from start_node"""
        visited = set()
        current_level = {start_node}

        for _ in range(max_depth):
            next_level = set()
            for node in current_level:
                for child in self.get_children(node):
                    if child not in visited:
                        visited.add(child)
                        next_level.add(child)
            current_level = next_level

        return visited

    def get_n_degree_dependencies_from_roots(self, max_depth=1):
        """Return a mapping of root -> set of dependencies within max_depth"""
        root_deps = {}
        roots = self.get_root_nodes()

        for root in roots:
            deps = self.get_descendants(root, max_depth)
            root_deps[root] = deps

        return root_deps
