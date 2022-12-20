from collections import abc
import functools
from typing import Union, Mapping, Any, Callable


from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_base import Spec, TypeSpec
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.typing import SpecType


def _convert_to_canonical_format(spec: SpecType) -> Union[Spec, SpecDict]:
    # convert spec of form list of nested_keys to model_spec with None leaves
    if isinstance(spec, list):
        spec = [(k,) if isinstance(k, str) else k for k in spec]
        return SpecDict({k: None for k in spec})

    # convert spec of form tree of constraints to model_spec
    if isinstance(spec, abc.Mapping):
        spec = SpecDict(spec)
        for key in spec:
            # if values are types or tuple of types, convert to TypeSpec
            if isinstance(spec[key], (type, tuple)):
                spec[key] = TypeSpec(spec[key])
        return spec

    if isinstance(spec, type):
        return TypeSpec(spec)

    # otherwise, assume spec is already in canonical format
    return spec


def _should_validate(cls_instance, method, tag: str = "input"):
    cache_store = getattr(cls_instance, f"__checked_{tag}_specs_cache__", None)
    return cache_store is None or method.__name__ not in cache_store


def _validate(
    *,
    cls_instance: object,
    method: Callable,
    data: Any,
    spec: Spec,
    filter: bool = False,
    tag: str = "input",
):
    is_mapping = isinstance(spec, SpecDict)
    cache_miss = _should_validate(cls_instance, method, tag=tag)

    if is_mapping:
        if not isinstance(data, Mapping):
            raise ValueError(f"{tag} must be a Mapping, got {type(data).__name__}")
        if cache_miss or filter:
            data = NestedDict(data)

    if cache_miss:
        try:
            spec.validate(data)
        except ValueError as e:
            raise ValueError(
                f"{tag} spec validation failed on "
                f"{cls_instance.__class__.__name__}.{method.__name__}, {e}."
            )

    return data


def check_input_specs(
    input_spec: str,
    *,
    filter: bool = False,
    cache: bool = False,
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, input_data, **kwargs):
            if cache and not hasattr(self, "__checked_input_specs_cache__"):
                self.__checked_input_specs_cache__ = {}

            input_data_ = input_data
            if input_spec:
                input_spec_ = getattr(self, input_spec)
                input_spec_ = _convert_to_canonical_format(input_spec_)
                input_data_ = _validate(
                    cls_instance=self,
                    method=func,
                    data=input_data,
                    spec=input_spec_,
                    filter=filter,
                    tag="input",
                )

                if filter and isinstance(input_data_, NestedDict):
                    # filtering should happen regardless of cache
                    input_data_ = input_data_.filter(input_spec_)

            output_data = func(self, input_data_, **kwargs)

            if cache and func.__name__ not in self.__checked_input_specs_cache__:
                self.__checked_input_specs_cache__[func.__name__] = True

            return output_data

        wrapper.__checked_input_specs__ = True
        return wrapper

    return decorator


def check_output_specs(
    output_spec: str,
    *,
    cache: bool = False,
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, input_data, **kwargs):
            if cache and not hasattr(self, "__checked_output_specs_cache__"):
                self.__checked_output_specs_cache__ = {}

            output_data = func(self, input_data, **kwargs)

            if output_spec:
                output_spec_ = getattr(self, output_spec)
                output_spec_ = _convert_to_canonical_format(output_spec_)
                _validate(
                    cls_instance=self,
                    method=func,
                    data=output_data,
                    spec=output_spec_,
                    tag="output",
                )

            if cache and func.__name__ not in self.__checked_output_specs_cache__:
                self.__checked_output_specs_cache__[func.__name__] = True

            return output_data

        wrapper.__checked_output_specs__ = True
        return wrapper

    return decorator
