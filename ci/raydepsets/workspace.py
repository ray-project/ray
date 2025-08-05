import yaml
from dataclasses import dataclass, field
from typing import List, Optional
import os


@dataclass
class Depset:
    name: str
    operation: str
    requirements: List[str]
    constraints: List[str]
    output: str
    override_flags: List[str]
    append_flags: List[str]
    source_depset: Optional[str] = None
    depsets: Optional[List[str]] = None


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> "Config":
        raw_depsets = data.get("depsets", [])
        depsets = [
            Depset(
                name=values.get("name"),
                requirements=values.get("requirements", []),
                constraints=values.get("constraints", []),
                operation=values.get("operation", "compile"),
                output=values.get("output"),
                source_depset=values.get("source_depset"),
                override_flags=values.get("override_flags", []),
                append_flags=values.get("append_flags", []),
                depsets=values.get("depsets", []),
            )
            for values in raw_depsets
        ]

        return Config(depsets=depsets)


class Workspace:
    def __init__(self, dir: str = None):
        self.dir = (
            dir if dir is not None else os.getenv("BUILD_WORKSPACE_DIRECTORY", None)
        )
        if self.dir is None:
            raise RuntimeError("BUILD_WORKSPACE_DIRECTORY is not set")

    def load_config(self, path: str) -> Config:
        with open(os.path.join(self.dir, path), "r") as f:
            data = yaml.safe_load(f)
            return Config.from_dict(data)
