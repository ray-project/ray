import logging
import json

from ray.util.annotations import PublicAPI
from abc import ABC, abstractmethod, abstractstaticmethod
from ray._private.runtime_env_prototype.pluggability.plugin_schema_manager import (
    RuntimeEnvPluginSchemaManager,
)
from typing import Any

logger = logging.getLogger(__name__)


class RuntimeEnvBase(ABC):
    """ The abstract class which makes sure the class could be converted to/from json.
    """
    @abstractmethod
    def to_jsonable_type(self):
        """ Convert class to a jsonable type, e.g. dict, list, string and so on.
        """
        raise NotImplementedError()

    @abstractstaticmethod
    def from_jsonable_type(jsonable_data) -> "RuntimeEnvBase":
        """ Convert from jsonable type.
        """
        raise NotImplementedError()


@PublicAPI
class RuntimeEnv(dict):
    def __init__(self):
        super().__init__()

    def set(self, runtime_env_name, typed_runtime_env):
        # Maybe we can do this automatically using `typed_runtime_env.__dict__` ?
        jsonable_type = typed_runtime_env.to_jsonable_type()
        RuntimeEnvPluginSchemaManager.validate(runtime_env_name, jsonable_type)
        self[runtime_env_name] = jsonable_type

    def __setitem__(self, key: str, value: Any) -> None:
        RuntimeEnvPluginSchemaManager.validate(key, value)
        return super().__setitem__(key, value)

    def get(self, runtime_env_name, runtime_env_type):
        return runtime_env_type.from_jsonable_type(self[runtime_env_name])
    
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
