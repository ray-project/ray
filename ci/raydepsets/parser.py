import re
from dataclasses import dataclass
from typing import List


@dataclass
class Dep:
    name: str
    version: str


def parse_lock_file(depset_path: str) -> List[Dep]:
    """Parse a depset lock file into a list of dependencies.

    Args:
        depset_path: The path to the depset lock file.

    Returns:
        A list of Dep objects. Each Dep object contains the name, version, and required_by dependencies.
    """
    deps = []
    with open(depset_path, "r") as f:
        for line in f:
            package_line_match = re.search(
                r"([A-Za-z0-9_.-]+)==([A-Za-z0-9.+-]+)", line
            )
            if package_line_match:
                name, version = package_line_match.group(1), package_line_match.group(2)
                deps.append(Dep(name=name, version=version))
    return deps
