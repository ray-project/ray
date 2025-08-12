import os
from dataclasses import dataclass, field
from string import Template
from typing import Any, Dict, List, Optional, Tuple

import yaml


@dataclass
class BuildArgSet:
    name: str
    build_args: Dict[str, str]


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
    build_arg_set_name: Optional[str] = None


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


def _dict_to_depset(depset: dict, build_arg_set_name: Optional[str] = None) -> Depset:
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
        build_arg_set_name=build_arg_set_name,
    )


@dataclass
class Config:
    @staticmethod
    def to_depset_map(
        data: dict, build_arg_sets: List[BuildArgSet]
    ) -> Dict[Tuple[str, str], Depset]:
        """
        Convert a config dict to a depset map.
        The depset map is a dictionary of tuples of (depset_name, build_arg_set_name) to Depset objects.
        """
        raw_depsets = data.get("depsets", [])
        depset_map = {}
        for depset in raw_depsets:
            # check if the depset has a build_arg_sets field
            build_arg_set_matrix = depset.get("build_arg_sets", [])
            if build_arg_set_matrix:
                expanded_depsets = _expand_depsets_per_build_arg_set(
                    depset, build_arg_set_matrix, build_arg_sets
                )
                for expanded_depset in expanded_depsets:
                    depset_id = (
                        expanded_depset.name,
                        expanded_depset.build_arg_set_name,
                    )
                    depset_map[depset_id] = expanded_depset
            else:
                depset_id = (depset["name"], None)
                depset_map[depset_id] = _dict_to_depset(depset=depset)
        return depset_map

    @staticmethod
    def parse_build_arg_sets(build_arg_sets: List[dict]) -> List[BuildArgSet]:
        return [
            BuildArgSet(
                name=build_arg_set.get("name", None),
                build_args=build_arg_set.get("build_args", []),
            )
            for build_arg_set in build_arg_sets
        ]


def _expand_depsets_per_build_arg_set(
    depset: dict,
    build_arg_set_matrix: List[str],
    build_arg_sets: Dict[str, BuildArgSet],
) -> List[Depset]:
    """returns a list of depsets expanded per build arg set"""
    expanded_depsets = []
    for build_arg_set_name in build_arg_set_matrix:
        build_arg_set = next(
            (
                build_arg_set
                for build_arg_set in build_arg_sets
                if build_arg_set.name == build_arg_set_name
            ),
            None,
        )
        if build_arg_set is None:
            raise KeyError(f"Build arg set {build_arg_set_name} not found")
        depset_yaml = _substitute_build_args(depset, build_arg_set)
        expanded_depsets.append(_dict_to_depset(depset_yaml, build_arg_set_name))
    return expanded_depsets


class Workspace:
    build_arg_sets: List[BuildArgSet] = field(default_factory=list)

    def __init__(self, dir: str = None):
        self.dir = (
            dir if dir is not None else os.getenv("BUILD_WORKSPACE_DIRECTORY", None)
        )
        if self.dir is None:
            raise RuntimeError("BUILD_WORKSPACE_DIRECTORY is not set")

    def build_depset_map(self, path: str) -> Dict[Tuple[str, str], Depset]:
        with open(os.path.join(self.dir, path), "r") as f:
            data = yaml.safe_load(f)
            self.build_arg_sets = Config.parse_build_arg_sets(
                data.get("build_arg_sets", [])
            )
            return Config.to_depset_map(data, self.build_arg_sets)
