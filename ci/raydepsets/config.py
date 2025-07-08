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
    source_depset: str


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)
    workspace_dir: str = field(default_factory=str)

    @staticmethod
    def from_dict(data: dict) -> "Config":
        Config.workspace_dir = get_current_directory()
        raw_depsets = data.get("depsets", [])
        depsets = [
            Depset(
                name=values.get("name"),
                requirements=[
                    os.path.join(Config.workspace_dir, requirement)
                    for requirement in values.get("requirements", [])
                ],
                constraints=[
                    os.path.join(Config.workspace_dir, constraint)
                    for constraint in values.get("constraints", [])
                ],
                operation=values.get("operation", "compile"),
                source_depset=values.get("source_depset", None),
                output=os.path.join(Config.workspace_dir, values.get("output")),
            )
            for values in raw_depsets
        ]

        return Config(depsets=depsets)


def get_current_directory() -> str:
    workspace_dir = os.environ.get("BUILD_WORKING_DIRECTORY")
    if workspace_dir:
        current_directory = workspace_dir
    else:
        current_directory = os.getcwd()
    return current_directory


def load_config(path: str) -> Config:
    with open(os.path.join(get_current_directory(), path), "r") as f:
        data = yaml.safe_load(f)
        return Config.from_dict(data)
