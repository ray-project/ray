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
    def from_dict(data: dict) -> "Config":
        current_directory = get_current_directory()
        raw_depsets = data.get("depsets", [])
        depsets = [
            Depset(
                name=values.get("name"),
                requirements=[
                    os.path.join(current_directory, requirement)
                    for requirement in values.get("requirements", [])
                ],
                constraints=[
                    os.path.join(current_directory, constraint)
                    for constraint in values.get("constraints", [])
                ],
                operation=values.get("operation", "compile"),
                output=os.path.join(current_directory, values.get("output")),
            )
            for values in raw_depsets
        ]

        return Config(depsets=depsets)


def get_current_directory() -> str:
    workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", os.getcwd())
    return workspace_dir


def load_config(path: str) -> Config:
    with open(os.path.join(get_current_directory(), path), "r") as f:
        data = yaml.safe_load(f)
        return Config.from_dict(data)


class Workspace:
    def __init__(self, dir: str):
        self.dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", dir)
        if self.dir is None:
            raise Exception("BUILD_WORKSPACE_DIRECTORY is not set")

    def load_config(self, path: str) -> Config:
        with open(os.path.join(self.dir, path), "r") as f:
            data = yaml.safe_load(f)
            return Config.from_dict(data)

# use temp dir for testing /tmp/
# copy test config into tmp dir
# update bazel rule