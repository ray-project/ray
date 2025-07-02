import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import os

@dataclass
class Depset:
    name: str
    operation: str
    requirements: List[str]
    constraints: List[str]
    degree: Optional[int] = None
    output: Optional[str] = None
    flags: Optional[List[str]] = field(default_factory=list)
    depset: Optional[List[str]] = field(default_factory=list)
    packages: Optional[List[str]] = field(default_factory=list)
    depsets: Optional[List[str]] = field(default_factory=list)


@dataclass
class Config:
    depsets: Dict[str, Depset] = field(default_factory=dict)

    @staticmethod
    def from_dict(data: dict) -> "Config":
        current_directory = get_current_directory()
        raw_depsets = data.get("depsets", {})
        depsets = {
            name: Depset(
                name=name,
                requirements=[os.path.join(current_directory, requirement) for requirement in values.get("requirements", [])],
                constraints=[os.path.join(current_directory, constraint) for constraint in values.get("constraints", [])],
                operation=values.get("operation", "compile"),
                output=os.path.join(current_directory, values.get("output")),
                flags=values.get("flags", []),
                depset=values.get("depset", []),
                packages=values.get("packages", []),
                depsets=values.get("depsets", []),
                degree=values.get("degree", None)
            )
            for name, values in raw_depsets.items()
        }

        return Config(
            depsets=depsets
        )

def get_current_directory() -> str:
    workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if workspace_dir:
        print(f"BUILD_WORKSPACE_DIRECTORY: {workspace_dir}")
        current_directory = workspace_dir
        print(f"Using Bazel workspace directory: {current_directory}")
    else:
        current_directory = os.getcwd()
        print(f"Using current directory: {current_directory}")
    return current_directory

def load_config(path: str) -> Config:

    print(f"testing: {get_current_directory()}")
    with open(os.path.join(get_current_directory(), path) , "r") as f:
        data = yaml.safe_load(f)
        return Config.from_dict(data)
