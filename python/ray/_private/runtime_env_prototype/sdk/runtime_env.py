import logging
import json

from ray.util.annotations import PublicAPI
from abc import ABC, abstractmethod, abstractstaticmethod
from ray._private.runtime_env_prototype.pluggability.plugin_schema_manager import (
    RuntimeEnvPluginSchemaManager,
)
from typing import Any
from dataclasses import dataclass, asdict, is_dataclass
from dacite import from_dict

logger = logging.getLogger(__name__)


@PublicAPI
class RuntimeEnv(dict):
    def __init__(self):
        super().__init__()

    def __setitem__(self, key: str, value: Any) -> None:
        if is_dataclass(value):
            jsonable_type = asdict(value)
        else:
            jsonable_type = value
        RuntimeEnvPluginSchemaManager.validate(key, jsonable_type)
        return super().__setitem__(key, jsonable_type)

    def set(self, runtime_env_name, typed_runtime_env):
        self.__setitem__(runtime_env_name, typed_runtime_env)

    def get(self, runtime_env_name, data_class=None):
        if not data_class:
            return self[runtime_env_name]
        else:
            return from_dict(data_class=data_class, data=self[runtime_env_name])
    
    def remove(self, runtime_env_name):
        del self[runtime_env_name]

    def serialize(self) -> str:
        return json.dumps(self, sort_keys=True)

    @staticmethod
    def deserialize(serialized_runtime_env: str) -> "RuntimeEnv":
        runtime_env_dict = json.loads(serialized_runtime_env)
        runtime_env = RuntimeEnv()
        runtime_env.update(runtime_env_dict)
        return runtime_env
