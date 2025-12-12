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

    def parse(self) -> List[Dep]:
        #
        with open(self.depset_path, "r") as f:
            data = f.read()
        print(data)
        return [
            Dep(name=name, version=version, dependencies=dependencies)
            for name, version, dependencies in data.get("depsets", [])
            if isinstance(name, str)
            and isinstance(version, str)
            and isinstance(dependencies, list)
        ]
