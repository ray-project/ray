import yaml
from dataclasses import dataclass, field
from typing import List, Optional
import os
from string import Template


@dataclass
class BuildArgSet:
    name: str
    build_args: List[str]


@dataclass
class Depset:
    name: str
    operation: str
    requirements: List[str]
    constraints: List[str]
    output: str
    source_depset: Optional[str] = None
    build_args: BuildArgSet = None
    depsets: Optional[List[str]] = None


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)
    build_args: List[BuildArgSet] = field(default_factory=list)

    @staticmethod
    def parse_configs(configs: List[dict]) -> List["BuildArgSet"]:
        return [
            BuildArgSet(
                name=config.get("name", None),
                build_args=config.get("build_args", []),
            )
            for config in configs
        ]

    @staticmethod
    def from_dict(data: dict) -> "Config":
        build_arg_sets = Config.parse_configs(data.get("configs", []))
        depsets = []
        raw_depsets = data.get("depsets", [])
        for depset in raw_depsets:
            config_matrix = depset.get("configs", [])
            if config_matrix:
                for config_name in config_matrix:
                    build_arg_set = next(
                        (
                            build_arg_set
                            for build_arg_set in build_arg_sets
                            if build_arg_set.name == config_name
                        ),
                        None,
                    )
                    substituted_depset = Template(str(depset)).substitute(
                        build_arg_set.build_args
                    )
                    depset_yaml = yaml.safe_load(substituted_depset)
                    depsets.append(
                        Depset(
                            name=depset_yaml.get("name"),
                            requirements=depset_yaml.get("requirements", []),
                            constraints=depset_yaml.get("constraints", []),
                            operation=depset_yaml.get("operation", None),
                            output=depset_yaml.get("output"),
                            source_depset=depset_yaml.get("source_depset"),
                            depsets=depset_yaml.get("depsets", []),
                            build_args=build_arg_set,
                        )
                    )
            else:
                depsets.append(
                    Depset(
                        name=depset.get("name"),
                        requirements=depset.get("requirements", []),
                        constraints=depset.get("constraints", []),
                        operation=depset.get("operation", None),
                        output=depset.get("output"),
                        source_depset=depset.get("source_depset"),
                        depsets=depset.get("depsets", []),
                        build_args=None,
                    )
                )

        return Config(depsets=depsets, build_args_sets=build_arg_sets)


class Workspace:
    def __init__(self, dir: str = None):
        self.dir = (
            dir if dir is not None else os.getenv("BUILD_WORKSPACE_DIRECTORY", None)
        )
        if self.dir is None:
            raise RuntimeError("BUILD_WORKSPACE_DIRECTORY is not set")

    def load_config(self, path: str) -> Config:
        with open(os.path.join(self.dir, path), "r") as f:
            data = yaml.safe_load(f.read())
            return Config.from_dict(data)
