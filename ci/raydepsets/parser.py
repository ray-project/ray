import re
from typing import List

from ci.raydepsets.models import Dep


class Parser:
    def __init__(self, depset_path: str):
        self.depset_path = depset_path

    def parse(self) -> List[Dep]:
        """Parse a depset lock file into a list of dependencies.

        Args:
            depset_path: The path to the depset lock file.

        Returns:
            A list of Dep objects. Each Dep object contains the name, version, and required_by dependencies.
        """
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
                # match either "# via <package>" or "#   <package>"
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
                    and (
                        dependency_line_match.group(1) or dependency_line_match.group(2)
                    )
                ):
                    # group 1: "# via <package>" or group 2: "#   <package>"
                    dep = dependency_line_match.group(1) or dependency_line_match.group(
                        2
                    )
                    if dep != "":
                        deps_arr.append(dep)
            # handle last dependency
            if name and version:
                deps.append(Dep(name=name, version=version, required_by=deps_arr))
        return deps
