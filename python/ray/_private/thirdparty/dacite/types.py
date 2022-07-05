from dataclasses import InitVar
from typing import Type, Any, Optional, Union, Collection, TypeVar, Dict, Callable, Mapping, List, Tuple

T = TypeVar("T", bound=Any)


def transform_value(
    type_hooks: Dict[Type, Callable[[Any], Any]], cast: List[Type], target_type: Type, value: Any
) -> Any:
    if target_type in type_hooks:
        value = type_hooks[target_type](value)
    else:
        for cast_type in cast:
            if is_subclass(target_type, cast_type):
                if is_generic_collection(target_type):
                    value = extract_origin_collection(target_type)(value)
                else:
                    value = target_type(value)
                break
    if is_optional(target_type):
        if value is None:
            return None
        target_type = extract_optional(target_type)
        return transform_value(type_hooks, cast, target_type, value)
    if is_generic_collection(target_type) and isinstance(value, extract_origin_collection(target_type)):
        collection_cls = value.__class__
        if issubclass(collection_cls, dict):
            key_cls, item_cls = extract_generic(target_type, defaults=(Any, Any))
            return collection_cls(
                {
                    transform_value(type_hooks, cast, key_cls, key): transform_value(type_hooks, cast, item_cls, item)
                    for key, item in value.items()
                }
            )
        item_cls = extract_generic(target_type, defaults=(Any,))[0]
        return collection_cls(transform_value(type_hooks, cast, item_cls, item) for item in value)
    return value


def extract_origin_collection(collection: Type) -> Type:
    try:
        return collection.__extra__
    except AttributeError:
        return collection.__origin__


def is_optional(type_: Type) -> bool:
    return is_union(type_) and type(None) in extract_generic(type_)


def extract_optional(optional: Type[Optional[T]]) -> T:
    for type_ in extract_generic(optional):
        if type_ is not type(None):
            return type_
    raise ValueError("can not find not-none value")


def is_generic(type_: Type) -> bool:
    return hasattr(type_, "__origin__")


def is_union(type_: Type) -> bool:
    return is_generic(type_) and type_.__origin__ == Union


def is_literal(type_: Type) -> bool:
    try:
        from typing import Literal  # type: ignore

        return is_generic(type_) and type_.__origin__ == Literal
    except ImportError:
        return False


def is_new_type(type_: Type) -> bool:
    return hasattr(type_, "__supertype__")


def extract_new_type(type_: Type) -> Type:
    return type_.__supertype__


def is_init_var(type_: Type) -> bool:
    return isinstance(type_, InitVar) or type_ is InitVar


def extract_init_var(type_: Type) -> Union[Type, Any]:
    try:
        return type_.type
    except AttributeError:
        return Any


def is_instance(value: Any, type_: Type) -> bool:
    if type_ == Any:
        return True
    elif is_union(type_):
        return any(is_instance(value, t) for t in extract_generic(type_))
    elif is_generic_collection(type_):
        origin = extract_origin_collection(type_)
        if not isinstance(value, origin):
            return False
        if not extract_generic(type_):
            return True
        if isinstance(value, tuple):
            tuple_types = extract_generic(type_)
            if len(tuple_types) == 1 and tuple_types[0] == ():
                return len(value) == 0
            elif len(tuple_types) == 2 and tuple_types[1] is ...:
                return all(is_instance(item, tuple_types[0]) for item in value)
            else:
                if len(tuple_types) != len(value):
                    return False
                return all(is_instance(item, item_type) for item, item_type in zip(value, tuple_types))
        if isinstance(value, Mapping):
            key_type, val_type = extract_generic(type_, defaults=(Any, Any))
            for key, val in value.items():
                if not is_instance(key, key_type) or not is_instance(val, val_type):
                    return False
            return True
        return all(is_instance(item, extract_generic(type_, defaults=(Any,))[0]) for item in value)
    elif is_new_type(type_):
        return is_instance(value, extract_new_type(type_))
    elif is_literal(type_):
        return value in extract_generic(type_)
    elif is_init_var(type_):
        return is_instance(value, extract_init_var(type_))
    elif is_type_generic(type_):
        return is_subclass(value, extract_generic(type_)[0])
    else:
        try:
            # As described in PEP 484 - section: "The numeric tower"
            if isinstance(value, (int, float)) and type_ in [float, complex]:
                return True
            return isinstance(value, type_)
        except TypeError:
            return False


def is_generic_collection(type_: Type) -> bool:
    if not is_generic(type_):
        return False
    origin = extract_origin_collection(type_)
    try:
        return bool(origin and issubclass(origin, Collection))
    except (TypeError, AttributeError):
        return False


def extract_generic(type_: Type, defaults: Tuple = ()) -> tuple:
    try:
        if hasattr(type_, "_special") and type_._special:
            return defaults
        return type_.__args__ or defaults  # type: ignore
    except AttributeError:
        return defaults


def is_subclass(sub_type: Type, base_type: Type) -> bool:
    if is_generic_collection(sub_type):
        sub_type = extract_origin_collection(sub_type)
    try:
        return issubclass(sub_type, base_type)
    except TypeError:
        return False


def is_type_generic(type_: Type) -> bool:
    try:
        return type_.__origin__ in (type, Type)
    except AttributeError:
        return False
