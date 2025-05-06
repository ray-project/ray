import copy
import inspect
from typing import Any, Dict, List, Type, TypeVar

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from ray.llm._internal.serve.config_generator.utils.prompt import (
    BoldIntPrompt,
    BoldPrompt,
)

Model = TypeVar("Model", bound=BaseModel)


def _is_type_or_subclass(obj, target_type) -> bool:
    """Check if obj is target_type or a subclass of target_type."""
    return obj is target_type or (inspect.isclass(obj) and issubclass(obj, target_type))


def _check_type_in_optional(type_obj, target_type) -> bool:
    """Check if an Optional type contains target_type or its subclass."""
    if (
        hasattr(type_obj, "__args__")
        and len(type_obj.__args__) == 2
        and type(None) in type_obj.__args__
    ):
        # Check each non-None argument
        for arg in type_obj.__args__:
            if arg is not type(None) and _is_type_or_subclass(arg, target_type):
                return True
    return False


def _is_type_expected(field_info: FieldInfo, target_type) -> bool:
    """Generic function to check if a field expects a specific type or its subclasses.

    This handles both direct types and Optional[type] types.
    """
    # For Pydantic v2
    if hasattr(field_info, "annotation"):
        annotation = field_info.annotation
        # Direct type check
        if _is_type_or_subclass(annotation, target_type):
            return True
        # Optional type check
        if _check_type_in_optional(annotation, target_type):
            return True

    # For Pydantic v1
    elif hasattr(field_info, "type_"):
        type_ = field_info.type_
        # Direct type check
        if _is_type_or_subclass(type_, target_type):
            return True
        # Optional type check
        if _check_type_in_optional(type_, target_type):
            return True

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
    # Check if we're using Pydantic v1 or v2
    cls_fields = getattr(model_cls, "model_fields", None) or getattr(
        model_cls, "__fields__", {}
    )

    defaults = defaults or {}
    for field in fields:
        if field in cls_fields:
            field_info = cls_fields[field]
            # Get description - handle both v1 and v2
            description = ""
            if hasattr(field_info, "description"):  # v2
                description = field_info.description or ""
            elif hasattr(field_info, "field_info") and hasattr(
                field_info.field_info, "description"
            ):  # v1
                description = field_info.field_info.description or ""

            # Get default - handle both v1 and v2
            default = defaults.get(field) or field_info.default

            if _is_type_expected(field_info, int) or _is_type_expected(
                field_info, float
            ):
                val = BoldIntPrompt.ask(
                    f"{description} Please provide a value for {field}", default=default
                )
                val = float(val)
            else:
                val = BoldPrompt.ask(
                    f"{description} Please provide a value for {field}", default=default
                )
            overrides[field] = val
    return overrides
