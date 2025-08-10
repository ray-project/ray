import os
from dataclasses import dataclass, field
from string import Template
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class BuildArgSet:
    name: str
    build_args: Dict[str, Any]


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
    build_arg_sets: Optional[List[str]] = None


def _substitute_build_args(obj: Any, build_arg_set: BuildArgSet):
    if isinstance(obj, str):
        return Template(obj).substitute(build_arg_set.build_args)
    elif isinstance(obj, dict):
        return {
            key: _substitute_build_args(value, build_arg_set)
            for key, value in obj.items()
        }
    elif isinstance(obj, list):
        return [_substitute_build_args(item, build_arg_set) for item in obj]
    else:
        return obj


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)
    build_arg_sets: List[BuildArgSet] = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> "Config":
        build_arg_sets = Config.parse_build_arg_sets(data.get("build_arg_sets", []))
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
                build_arg_sets=values.get("build_arg_sets", []),
            )
            for values in raw_depsets
        ]

        return Config(depsets=depsets, build_arg_sets=build_arg_sets)

    @staticmethod
    def parse_build_arg_sets(build_arg_sets: List[dict]) -> List[BuildArgSet]:
        return [
            BuildArgSet(
                name=build_arg_set.get("name", None),
                build_args=build_arg_set.get("build_args", []),
            )
            for build_arg_set in build_arg_sets
        ]


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
