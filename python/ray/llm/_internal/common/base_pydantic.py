from typing import Type, TypeVar

import yaml
from pydantic import BaseModel, ConfigDict

ModelT = TypeVar("ModelT", bound=BaseModel)


class BaseModelExtended(BaseModel):
    # NOTE(edoakes): Pydantic protects the namespace `model_` by default and prints
    # warnings if you define fields with that prefix. However, we added such fields
    # before this behavior existed. To avoid spamming user-facing logs, we mark the
    # namespace as not protected. This means we need to be careful about overriding
    # internal attributes starting with `model_`.
    # See: https://github.com/anyscale/ray-llm/issues/1425
    model_config = ConfigDict(
        protected_namespaces=tuple(),
        extra="forbid",
    )

    @classmethod
    def parse_yaml(cls: Type[ModelT], file, **kwargs) -> ModelT:
        kwargs.setdefault("Loader", yaml.SafeLoader)
        dict_args = yaml.load(file, **kwargs)
        return cls.model_validate(dict_args)

    @classmethod
    def from_file(cls: Type[ModelT], path: str, **kwargs) -> ModelT:
        """Load a model from a YAML file path."""
        with open(path, "r") as f:
            return cls.parse_yaml(f, **kwargs)
