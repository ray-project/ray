from typing import List, Dict, Optional

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
    def from_requirement(cls, requirement: str) -> 'Dep':
        # Simple parsing of requirement strings like "package==1.0.0" or "package>=1.0.0"
        parts = requirement.split('==')
        if len(parts) == 2:
            return cls(parts[0], ['=='], parts[1])
        parts = requirement.split('>=')
        if len(parts) == 2:
            return cls(parts[0], ['>='], parts[1])
        parts = requirement.split('<=')
        if len(parts) == 2:
            return cls(parts[0], ['<='], parts[1])
        return cls(requirement, [], "")

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "constraints": self.constraints,
            "version": self.version
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Dep':
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
                if line and not line.startswith('#'):
                    self.dependencies.append(Dep.from_requirement(line))

    def compile(self, constraints: List[str], set: Optional['DepSet'] = None):
        # Creates a depset from a list of constraint files or an existing depset
        pass

    def subset(self, packages: List[str]) -> 'DepSet':
        # Creates a depset based on existing set and a set of package names (min dep set)
        pass

    def expand(self, deps: List[Dep], constraints: List[str]) -> 'DepSet':
        # Creates a new expanded depset based on 1 or more depsets and 1 or more constraint files
        pass

    def relax(self, degree: int) -> 'DepSet':
        # Converts a set back into versioned constraints - keeping select dep versions pinned
        pass

    def to_dict(self) -> Dict:
        return {
            "requirements_fp": self.requirements_fp,
            "dependencies": [dep.to_dict() for dep in self.dependencies]
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'DepSet':
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

