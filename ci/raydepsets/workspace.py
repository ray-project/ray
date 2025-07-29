import yaml
from dataclasses import dataclass, field
from typing import List, Optional
import os
from string import Template


@dataclass
class Env:
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
    env: Env = None


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> "Config":
        depsets = []
        raw_depsets = data.get("depsets", [])
        raw_envs = data.get("envs", [])
        for env in raw_envs:
            build_args = env.get("build_args", {})
            substituted_depsets = Template(str(raw_depsets)).substitute(build_args)
            depsets_yaml = yaml.safe_load(substituted_depsets)

            depsets.extend(
                [
                    Depset(
                        name=values.get("name"),
                        requirements=values.get("requirements", []),
                        constraints=values.get("constraints", []),
                        operation=values.get("operation", "compile"),
                        output=values.get("output"),
                        source_depset=values.get("source_depset"),
                        env=Env(
                            name=env.get("name"),
                            build_args=env.get("build_args", []),
                        ),
                    )
                    for values in depsets_yaml
                ]
            )

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
            data = yaml.safe_load(f.read())
            return Config.from_dict(data)
