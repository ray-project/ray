import yaml
from dataclasses import dataclass, field
from typing import List
import os
from typing import Optional


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
        output_dir = os.path.join(workspace_dir, data.get("output_dir", ""))
        print(f"output_dir: {output_dir}")
        depsets = [
            Depset(
                name=values.get("name"),
                requirements=[
                    requirement for requirement in values.get("requirements", [])
                ],
                constraints=[
                    constraint for constraint in values.get("constraints", [])
                ],
                operation=values.get("operation", "compile"),
                output=values.get("output"),
            )
            for values in raw_depsets
        ]

        return Config(depsets=depsets)


class Workspace:
    def __init__(self, dir: str = None):
        print(f"directory: {dir}")
        self.dir = (
            dir if dir is not None else os.getenv("BUILD_WORKSPACE_DIRECTORY", None)
        )
        if self.dir is None:
            raise Exception("BUILD_WORKSPACE_DIRECTORY is not set")

    def load_config(self, path: str) -> Config:
        with open(os.path.join(self.dir, path), "r") as f:
            data = yaml.safe_load(f)
            return Config.from_dict(data, self.dir)
