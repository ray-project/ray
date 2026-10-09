from dataclasses import Field, MISSING, _FIELDS, _FIELD, _FIELD_INITVAR  # type: ignore
from typing import Type, Any, TypeVar, List

from .data import Data
from .types import is_optional

T = TypeVar("T", bound=Any)


class DefaultValueNotFoundError(Exception):
    pass


def get_default_value_for_field(field: Field) -> Any:
    if field.default != MISSING:
        return field.default
    elif field.default_factory != MISSING:  # type: ignore
        return field.default_factory()  # type: ignore
    elif is_optional(field.type):
        return None
    raise DefaultValueNotFoundError()


def create_instance(data_class: Type[T], init_values: Data, post_init_values: Data) -> T:
    instance = data_class(**init_values)
    for key, value in post_init_values.items():
        setattr(instance, key, value)
    return instance


def get_fields(data_class: Type[T]) -> List[Field]:
    fields = getattr(data_class, _FIELDS)
    return [f for f in fields.values() if f._field_type is _FIELD or f._field_type is _FIELD_INITVAR]
