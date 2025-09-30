import os
from dataclasses import dataclass, field
from string import Template
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class BuildArgSet:
    build_args: Dict[str, str]


@dataclass
class Depset:
    name: str
    operation: str
    output: str
    constraints: Optional[List[str]] = None
    override_flags: Optional[List[str]] = None
    append_flags: Optional[List[str]] = None
    requirements: Optional[List[str]] = None
    packages: Optional[List[str]] = None
    source_depset: Optional[str] = None
    depsets: Optional[List[str]] = None
    pre_hooks: Optional[List[str]] = None


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


def _dict_to_depset(depset: dict) -> Depset:
    return Depset(
        name=depset.get("name"),
        requirements=depset.get("requirements", []),
        constraints=depset.get("constraints", []),
        operation=depset.get("operation", None),
        output=depset.get("output"),
        source_depset=depset.get("source_depset"),
        depsets=depset.get("depsets", []),
        override_flags=depset.get("override_flags", []),
        append_flags=depset.get("append_flags", []),
        pre_hooks=depset.get("pre_hooks", []),
        packages=depset.get("packages", []),
    )


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)
    build_arg_sets: Dict[str, BuildArgSet] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        build_arg_sets = cls.parse_build_arg_sets(data.get("build_arg_sets", {}))
        raw_depsets = data.get("depsets", [])
        depsets = []
        for depset in raw_depsets:
            build_arg_set_keys = depset.get("build_arg_sets", [])
            if build_arg_set_keys:
                # Expand the depset for each build arg set
                for build_arg_set_key in build_arg_set_keys:
                    build_arg_set = build_arg_sets[build_arg_set_key]
                    if build_arg_set is None:
                        raise KeyError(f"Build arg set {build_arg_set_key} not found")
                    depset_yaml = _substitute_build_args(depset, build_arg_set)
                    depsets.append(_dict_to_depset(depset_yaml))
            else:
                depsets.append(_dict_to_depset(depset))
        return Config(depsets=depsets, build_arg_sets=build_arg_sets)

    @staticmethod
    def parse_build_arg_sets(build_arg_sets: Dict[str, dict]) -> Dict[str, BuildArgSet]:
        return {
            key: BuildArgSet(
                build_args=build_arg_set,
            )
            for key, build_arg_set in build_arg_sets.items()
        }


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
