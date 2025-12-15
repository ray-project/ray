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
                if len(deps_arr) > 0 and package_line_match:
                    deps.append(Dep(name=name, version=version, required_by=deps_arr))
                    deps_arr = []

                dependency_line_match = re.search(r"^\s*#\s+(.*)$", line)
                if package_line_match:
                    name, version = package_line_match[0]
                elif dependency_line_match and dependency_line_match.group(1) != "via":
                    deps_arr.append(dependency_line_match.group(1).replace("via ", ""))

        return deps
