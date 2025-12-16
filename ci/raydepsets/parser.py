import re
from dataclasses import dataclass
from typing import List


@dataclass
class Dep:
    name: str
    version: str
    required_by: List[str]


class Parser:
    def __init__(self, depset_path: str):
        self.depset_path = depset_path

    def parse(self) -> List[Dep]:
        deps = []
        with open(self.depset_path, "r") as f:
            deps_arr = []
            name = None
            version = None
            for line in f.readlines():
                package_line_match = re.findall(
                    r"([A-Za-z0-9_\-]+)==([A-Za-z0-9\.\-]+)", line
                )
                if name and version and package_line_match:
                    deps.append(Dep(name=name, version=version, required_by=deps_arr))
                    deps_arr = []
                dependency_line_match = re.search(
                    r"^\s{4}#\s{3}(.*)$|^\s{4}#\svia\s(.*)$", line
                )
                # ignore requirements and constraints
                req_line_match = re.search(r"-r\s+(.*)$", line)
                constraint_line_match = re.search(r"-c\s+(.*)$", line)
                if package_line_match:
                    name, version = package_line_match[0]
                elif (
                    dependency_line_match
                    and not req_line_match
                    and not constraint_line_match
                    and dependency_line_match.group(1) != ""
                    and dependency_line_match.group(2) != ""
                ):
                    # match either "# via <package>" or "#   <package>"
                    dep = dependency_line_match.group(1) or dependency_line_match.group(
                        2
                    )
                    deps_arr.append(dep)
            # handle last dependency
            deps.append(Dep(name=name, version=version, required_by=deps_arr))
        return deps
