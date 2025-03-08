import copy
from typing import Any, Dict, List, Type, TypeVar

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from ray.llm._internal.serve.config_generator.utils.prompt import (
    BoldIntPrompt,
    BoldPrompt,
)

Model = TypeVar("Model", bound=BaseModel)


def _is_int_type_expected(field_info: FieldInfo) -> bool:
    if field_info.annotation is int:
        return True
    elif (
        len(field_info.annotation.__args__) == 2
        and type(None) in field_info.annotation.__args__
        and int in field_info.annotation.__args__
    ):
        return True
    else:
        return False


def ask_and_merge_model_overrides(
    existing_configs: Dict[str, Any],
    model_cls: Type[Model],
    fields: List[str],
    defaults: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Given the existing values and the specified overrideable pydantic fields,
    this method constructs a new dict with the override values.
    """
    overrides = copy.deepcopy(existing_configs)
    cls_fields = model_cls.model_fields
    defaults = defaults or {}
    for field in fields:
        if field in cls_fields:
            description = cls_fields[field].description or ""
            default = defaults.get(field) or cls_fields[field].default
            if _is_int_type_expected(cls_fields[field]):
                val = BoldIntPrompt.ask(
                    f"{description} Please provide a value for {field}", default=default
                )
                val = int(val)
            else:
                val = BoldPrompt.ask(
                    f"{description} Please provide a value for {field}", default=default
                )
            overrides[field] = val
    return overrides
