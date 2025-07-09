import yaml
from dataclasses import dataclass, field
from typing import List
import os


@dataclass
class Depset:
    name: str
    operation: str
    requirements: List[str]
    constraints: List[str]
    output: str


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict, workspace_dir: str) -> "Config":
        raw_depsets = data.get("depsets", [])
        depsets = [
            Depset(
                name=values.get("name"),
                requirements=[
                    os.path.join(workspace_dir, requirement)
                    for requirement in values.get("requirements", [])
                ],
                constraints=[
                    os.path.join(workspace_dir, constraint)
                    for constraint in values.get("constraints", [])
                ],
                operation=values.get("operation", "compile"),
                output=os.path.join(workspace_dir, values.get("output")),
            )
            for values in raw_depsets
        ]

        return Config(depsets=depsets)


class Workspace:
    def __init__(self, dir: str = None):
        self.dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", dir)
        if self.dir is None:
            raise Exception("BUILD_WORKSPACE_DIRECTORY is not set")

    def load_config(self, path: str) -> Config:
        with open(os.path.join(self.dir, path), "r") as f:
            data = yaml.safe_load(f)
            return Config.from_dict(data, self.dir)
