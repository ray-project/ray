import logging
import json

from ray.util.annotations import PublicAPI
from abc import ABC, abstractmethod, abstractstaticmethod


logger = logging.getLogger(__name__)


class RuntimeEnvBase(ABC):
    @abstractmethod
    def to_jsonable_type(self):
        raise NotImplementedError()

    @abstractstaticmethod
    def from_jsonable_type(jsonable_data) -> "RuntimeEnvBase":
        raise NotImplementedError()


@PublicAPI
class RuntimeEnv(dict):
    def __init__(self):
        super().__init__()

    def set(self, runtime_env_name, typed_runtime_env):
        self[runtime_env_name] = typed_runtime_env.to_jsonable_type()

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
