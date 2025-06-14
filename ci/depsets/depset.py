from typing import List

import os
import tempfile
import numpy as np

class DepGraph:
    def __init__(self, deps: List[Dep]):
        self.deps = deps
        self.graph = {}

    def add_dep(self, dep: Dep):
        self.graph[dep.name] = dep

class DepSet:
    def __init__(self, constraints: List[Dep], requirements_fp: str):
        self.dependencies = constraints # resolve requirements.txt as deps and add them here

    def compile(self, constraints: List[str], set: DepSet):
        # Creates a depset from a list of constraint files or an existing depset
        pass

    def subset(self, set: DepSet, packages: List[str]):
        # Creates a depset based on existing set and a set of package names (min dep set)
        pass

    def expand(self, deps: List[Dep], constraints: List[str]):
        # Creates a new expanded depset based on 1 or more depsets and 1 or more constraint files
        pass
    def relax(self, degree: int):
        # Converts a set back into versioned constraints - keeping select dep versions pinned
        pass


class Dep:
     def __init__(self, name: str, constraints: List[str], version: str, c):
          self.name = name
          self.version = version
          self.constraints = constraints

     def __str__(self):
          return f"{self.name}{self.constraints}{self.version}"

     def __repr__(self):
          return self.__str__()