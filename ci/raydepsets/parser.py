import re
from dataclasses import dataclass
from typing import List


@dataclass
class Dep:
    name: str
    version: str
    dependencies: List[str]


class Parser:
    def __init__(self, depset_path: str):
        self.depset_path = depset_path
        # self.depset_path = "ci/raydepsets/tests/test_data/test_lock_file.lock"

    def parse(self) -> List[Dep]:
        deps = []
        with open(self.depset_path, "r") as f:
            for line in f.readlines():
                package_line_match = re.findall(
                    r"([A-Za-z0-9_\-]+)==([A-Za-z0-9\.\-]+)", line
                )
                dependency_line_match = re.findall(r"(#\s{3}(.*)$)", line)
                if package_line_match:
                    print("package line match: ", package_line_match[0])
                    name, version = package_line_match[0]
                if dependency_line_match:
                    print("dependency line match: ", dependency_line_match[0])
                    deps.append(
                        Dep(
                            name=name,
                            version=version,
                            dependencies=dependency_line_match[0],
                        )
                    )

        # matches = re.findall(r"([A-Za-z0-9_\-]+)==([A-Za-z0-9\.\-]+)", data)
        # # need to handle dependencies
        # if matches is None:
        #     raise ValueError("No matches found")
        # for name, version in matches:
        #     deps.append(Dep(name=name, version=version, dependencies=[]))

        return deps


if __name__ == "__main__":
    parser = Parser("ci/raydepsets/tests/test_data/test_lock_file.lock")
    print(parser.parse())
