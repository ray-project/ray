import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from string import Template
from typing import Any, Dict, List, Optional

import jsonschema
import yaml

_SCHEMA_PATH = Path(__file__).parent / "depsets.schema.json"
_operation_validators: Dict[str, jsonschema.Draft202012Validator] = {}


@dataclass
class BuildArgSet:
    build_args: Dict[str, str]


@dataclass
class Depset:
    name: str
    operation: str
    output: str
    config_name: str
    constraints: Optional[List[str]] = None
    override_flags: Optional[List[str]] = None
    append_flags: Optional[List[str]] = None
    requirements: Optional[List[str]] = None
    packages: Optional[List[str]] = None
    source_depset: Optional[str] = None
    depsets: Optional[List[str]] = None
    pre_hooks: Optional[List[str]] = None
    include_setuptools: Optional[bool] = False


def _get_operation_validators() -> Dict[str, jsonschema.Draft202012Validator]:
    """Load per-operation validators from depsets.schema.json, once."""
    if not _operation_validators:
        with open(_SCHEMA_PATH, "r") as f:
            schema = json.load(f)
        for operation in schema["$defs"]["depset"]["properties"]["operation"]["enum"]:
            # Validate directly against the operation's branch so errors point
            # at the offending field instead of a generic oneOf mismatch.
            _operation_validators[operation] = jsonschema.Draft202012Validator(
                {"$defs": schema["$defs"], "$ref": f"#/$defs/{operation}"}
            )
    return _operation_validators


def _format_schema_error(error: jsonschema.exceptions.ValidationError) -> str:
    if error.validator == "anyOf":
        options = sorted(
            req
            for option in error.validator_value
            for req in option.get("required", [])
        )
        message = f"must set at least one of: {', '.join(options)}"
    else:
        message = error.message
    if error.json_path != "$":
        return f"{error.json_path}: {message}"
    return message


def validate_depset(depset: dict, config_name: str) -> None:
    """Validate a raw depset dict against the schema for its operation."""
    name = depset.get("name", "<unnamed>")
    operation = depset.get("operation")
    validators = _get_operation_validators()
    if operation not in validators:
        raise ValueError(
            f"Invalid operation: {operation} for depset {name} in config {config_name}"
        )
    errors = sorted(
        validators[operation].iter_errors(depset), key=lambda e: e.json_path
    )
    if errors:
        details = "\n".join(f"  {_format_schema_error(e)}" for e in errors)
        raise ValueError(
            f"Invalid {operation} depset {name} in config {config_name}:\n{details}"
        )


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


def _dict_to_depset(depset: dict, config_name: str) -> Depset:
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
        include_setuptools=depset.get("include_setuptools", False),
        config_name=config_name,
    )


@dataclass
class Config:
    depsets: List[Depset] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict, config_name: str) -> "Config":
        build_arg_sets = cls.parse_build_arg_sets(data.get("build_arg_sets", {}))
        raw_depsets = data.get("depsets", [])
        depsets = []
        for depset in raw_depsets:
            validate_depset(depset, config_name)
            build_arg_set_keys = depset.get("build_arg_sets", [])
            if build_arg_set_keys:
                # Expand the depset for each build arg set
                for build_arg_set_key in build_arg_set_keys:
                    try:
                        build_arg_set = build_arg_sets[build_arg_set_key]
                    except KeyError:
                        raise KeyError(
                            f"Build arg set {build_arg_set_key} not found in config {config_name}"
                        )
                    depset_yaml = _substitute_build_args(depset, build_arg_set)
                    depsets.append(_dict_to_depset(depset_yaml, config_name))
            else:
                depsets.append(_dict_to_depset(depset, config_name))
        return Config(depsets=depsets)

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

    def load_configs(self, config_path: str) -> Config:
        merged_configs = self.merge_configs(self.get_all_configs(config_path))
        return merged_configs

    def get_all_configs(self, config_path: str) -> List[Config]:
        return [self.load_config(path) for path in self.get_configs_dir(config_path)]

    def get_configs_dir(self, configs_path: str) -> List[str]:
        configs_dir = os.path.dirname(os.path.join(self.dir, configs_path))
        return [
            os.path.join(self.dir, configs_dir, path)
            for path in os.listdir(os.path.join(self.dir, configs_dir))
            if path.endswith(".depsets.yaml")
        ]

    def load_config(self, config_path: str) -> Config:
        with open(os.path.join(self.dir, config_path), "r") as f:
            data = yaml.safe_load(f)
        config_name = os.path.basename(config_path)
        config = Config.from_dict(data, config_name)
        return config

    def merge_configs(self, configs: List[Config]) -> Config:
        return Config(
            depsets=[depset for config in configs for depset in config.depsets]
        )
