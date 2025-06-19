from __future__ import annotations
from typing import List, Dict
from collections import defaultdict
import os
import re

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
    def from_requirement(cls, requirement: str) -> Dep:
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
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> Dep:
        return cls(data["name"], data["constraints"], data["version"])


class DepSet:
    def __init__(self, requirements_fp: str):
        self.requirements_fp = requirements_fp
        self.dependencies: List[Dep] = []
        self.dep_dag: DependencyGraph = DependencyGraph()
        self._load_requirements()

    def _load_requirements(self):
        if not os.path.exists(self.requirements_fp):
            return
        with open(self.requirements_fp) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and not line.startswith("--hash"):
                    self.dependencies.append(
                        Dep.from_requirement(line.replace(" \\", ""))
                    )

    def to_dict(self) -> Dict:
        return {
            "requirements_fp": self.requirements_fp,
            "dependencies": [dep.to_dict() for dep in self.dependencies],
        }

    def to_txt(self) -> str:
        return "\n".join([str(dep) for dep in self.dependencies])

    @classmethod
    def from_dict(cls, data: Dict) -> DepSet:
        depset = cls(data["requirements_fp"])
        depset.dependencies = [
            Dep.from_dict(dep_data) for dep_data in data["dependencies"]
        ]
        return depset


class DependencyGraph:
    def __init__(self):
        # Each node points to a set of its children (dependencies)
        self.graph = defaultdict(set)

    def add_node(self, node):
        self.graph[node]  # Ensure node exists

    def add_edge(self, parent, child):
        """parent depends on child"""
        self.graph[parent].add(child)
        # self.graph[child.split("==")[0]]

    def get_dependencies(self, node):
        """Return direct dependencies of a node"""
        return self.graph.get(node, set())

    def get_all_nodes(self):
        return list(self.graph.keys())

    def __repr__(self):
        return "\n".join(f"{k} -> {sorted(v)}" for k, v in self.graph.items())

    def only_dependencies(self):
        return "\n".join(
            f"{k} -> {sorted(v)}" for k, v in self.graph.items() if v and len(v) > 0
        )

    def relax(self, degree: int) -> DependencyGraph:
        return_set = set()
        root_nodes = self.find_root_nodes()
        for root in root_nodes:
            return_set.update(self.get_nth_degree_depedencies(root, degree))
        return return_set

    def get_nth_degree_depedencies(self, root_node, degree: int):
        return_set = set()
        return_set.add(root_node)  # Always include the root node (degree 0)

        if degree > 0:
            for child in self.get_dependencies(root_node):
                # Include all dependencies from degree 0 to degree-1 for each child
                return_set.update(self.get_nth_degree_depedencies(child, degree - 1))
        return return_set

    def find_root_nodes(self):
        all_nodes = set(self.graph.keys())
        child_nodes = {child for children in self.graph.values() for child in children}
        root_nodes = all_nodes - child_nodes
        return root_nodes


def parse_compiled_requirements(file_path: str) -> DependencyGraph:
    graph = DependencyGraph()

    with open(file_path, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()

        # Match: name==version
        pkg_match = re.match(r"([\w\-\[\]]+)==([\d\.]+)", line)
        if pkg_match:
            name, version = pkg_match.groups()
            # key = f"{name}=={version}"
            key = name
            graph.add_node(key)

        else:
            # Match: name @ cachepath
            pkg_cache_match = re.match(r"([\w\-\[\]]+)\s*@\s*", line)
            if pkg_cache_match:
                name = pkg_cache_match.group(1)
                key = name

        # If it's a `# via` line
        via_match = re.match(r"#\s+via\s*$", line)
        if via_match:
            continue
        elif line.startswith("#") and not any(
            ext in line for ext in [".txt", ".in", ".lock", ".lockfile"]
        ):
            parent = line.strip("#").strip()
            graph.add_edge(parent, key)
            continue

    return graph
