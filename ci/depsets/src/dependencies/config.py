import yaml
from dataclasses import dataclass, field
from typing import List, Dict, Optional


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
        raw_depsets = data.get("depsets", {})
        depsets = {
            name: Depset(
                name=name,
                requirements=values.get("requirements", []),
                constraints=values.get("constraints", []),
                operation=values.get("operation", "compile"),
                output=values.get("output"),
                flags=values.get("flags", []),
                depset=values.get("depset", []),
                packages=values.get("packages", []),
                depsets=values.get("depsets", [])
            )
            for name, values in raw_depsets.items()
        }

        return Config(
            depsets=depsets
        )


def load_config(path: str) -> Config:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return Config.from_dict(data)
