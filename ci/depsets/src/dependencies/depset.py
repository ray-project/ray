from __future__ import annotations
from typing import List, Dict
from collections import defaultdict
import os
import re
import click
from typing import Set

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
                if line and not line.startswith("#") and not line.startswith("--hash"):
                    self.dependencies.append(
                        Dep.from_requirement(line.replace(" \\", ""))
                    )

    def to_file(self, output: str):
        with open(output, "w") as f:
            for dep in self.dependencies:
                f.write(f"{dep}\n")
