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
        self._build_dag()

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

    def _build_dag(self):
        # with open(f"{source}_deps.txt", "w") as f:
        #     for dep in source_depset.dependencies:
        #         f.write(f"{dep}\n")

        self.dep_dag = parse_compiled_requirements(
            self.requirements_fp
        )
        # with open(f"{source}_dag.txt", "w") as f:
        #     f.write(str(source_depset.dep_dag))

    def to_dict(self) -> Dict:
        return {
            "requirements_fp": self.requirements_fp,
            "dependencies": [dep.to_dict() for dep in self.dependencies],
        }

    def to_txt(self) -> str:
        return "\n".join([str(dep) for dep in self.dependencies])

    def to_file(self, output: str):
        with open(output, "w") as f:
            f.write(self.to_txt())

    def traverse(self, packages: List[str], new_depset: DepSet):
        for dep in self.dependencies:
            if dep.name in packages:
                new_depset.dep_dag.add_edge(dep.name, dep.name)
            else:
                self.traverse(packages, new_depset)

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

    def extract_subgraph_with_dependencies(self, packages: List[str]) -> 'DependencyGraph':
        """
        Traverse the dependency graph and return a new graph containing
        the mentioned packages and all their transitive dependencies.

        Args:
            packages: List of package names to include

        Returns:
            DependencyGraph: New graph with only the specified packages and their deps
        """
        subgraph = DependencyGraph()
        visited = set()

        def traverse_dependencies(package_name: str):
            """Recursively traverse dependencies of a package"""
            if package_name in visited:
                return

            visited.add(package_name)

            # Add the package to the subgraph
            subgraph.add_node(package_name)

            # Get direct dependencies of this package
            direct_deps = self.get_dependencies(package_name)

            # Add edges and recursively traverse dependencies
            for dep in direct_deps:
                subgraph.add_edge(package_name, dep)
                traverse_dependencies(dep)

        # Start traversal from each mentioned package
        for package in packages:
            if package in self.graph:
                traverse_dependencies(package)
            else:
                print(f"Warning: Package '{package}' not found in dependency graph")

        return subgraph

    def extract_subgraph_only_mentioned(self, packages: List[str]) -> 'DependencyGraph':
        """
        Extract a subgraph containing only the mentioned packages and their
        direct relationships to each other (no transitive dependencies).

        Args:
            packages: List of package names to include

        Returns:
            DependencyGraph: New graph with only specified packages and their interconnections
        """
        subgraph = DependencyGraph()
        package_set = set(packages)

        # Add all mentioned packages as nodes
        for package in packages:
            if package in self.graph:
                subgraph.add_node(package)
            else:
                print(f"Warning: Package '{package}' not found in dependency graph")

        # Add edges only between mentioned packages
        for package in packages:
            if package in self.graph:
                direct_deps = self.get_dependencies(package)
                for dep in direct_deps:
                    if dep in package_set:  # Only include if dependency is also mentioned
                        subgraph.add_edge(package, dep)

        return subgraph

    def to_depset(self) -> DepSet:
        # Add edges only between mentioned packages
        for package in self.graph:
            direct_deps = self.get_dependencies(package)
            for dep in direct_deps:
                self.add_edge(package, dep)

        return self

    def flatten_to_requirements(self, include_versions: bool = True) -> List[str]:
        """
        Flatten the dependency graph into a list of package requirements.

        Args:
            include_versions: Whether to include version constraints (if available)

        Returns:
            List[str]: Flat list of package requirements
        """
        all_packages = set()

        # Collect all unique packages from the graph
        for parent, deps in self.graph.items():
            all_packages.add(parent)
            all_packages.update(deps)

        # Convert to sorted list for consistent output
        return sorted(all_packages)

    def flatten_to_requirements_with_versions(self, depset: 'DepSet') -> List[str]:
        """
        Flatten the dependency graph into a requirements list with version information
        from the original DepSet.

        Args:
            depset: Original DepSet containing version information

        Returns:
            List[str]: Flat list of package requirements with versions
        """
        requirements = []
        all_packages = set()

        # Collect all unique packages from the graph
        for parent, deps in self.graph.items():
            all_packages.add(parent)
            all_packages.update(deps)

        # Create a mapping of package names to their Dep objects
        dep_map = {dep.name: dep for dep in depset.dependencies}

        # Build requirements list with version info
        for package in sorted(all_packages):
            if package in dep_map:
                # Use the original Dep object which has version info
                requirements.append(str(dep_map[package]))
            else:
                # Package not found in original depset, add without version
                requirements.append(package)

        return requirements

    def to_requirements_txt(self, output_file: str):
        """
        Write the flattened dependency graph to a requirements.txt file.

        Args:
            output_file: Path to output requirements.txt file
            depset: Optional original DepSet for version information
        """
        requirements = self.flatten_to_requirements(include_versions=True)

        with open(output_file, "w") as f:
            for req in requirements:
                f.write(f"{req}\n")

        return len(requirements)

# class DepsetConfig:
#     def __init__(self, file_path: str):
#         self.file_path = file_path
#         self.operation = operation
#         self.dependencies = dependencies
#         self.constraints = constraints

#     def to_dict(self) -> Dict:
#         return {
#             "file_path": self.file_path,
#             "operation": self.operation,
#             "dependencies": self.dependencies,
#         }

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
