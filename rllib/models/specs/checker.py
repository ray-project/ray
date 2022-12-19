
import functools
from typing import Union, Type, Mapping, Any, Tuple, Optional, List

from ray.util.annotations import PublicAPI, DeveloperAPI

from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_base import TensorSpec, SpecsAbstract, TypeSpec
from ray.rllib.models.specs.specs_dict import ModelSpec

NestedKeys = List[Union[str, Tuple[str, ...]]]
Constraint = Union[Type, Tuple[Type, ...], SpecsAbstract]
SupportedSpecs = Union[NestedKeys, NestedDict[Optional[Constraint]]]


def _convert_to_canonical_format(spec: SupportedSpecs) -> Union[SpecsAbstract, ModelSpec]:
    # convert spec of form list of nested_keys to model_spec with None leaves
    if isinstance(spec, list):
        spec = [(k,) if isinstance(k, str) else k for k in spec]
        return ModelSpec({k: None for k in spec})
    
    # convert spec of form tree of constraints to model_spec
    if isinstance(spec, Mapping):
        spec = ModelSpec(spec)
        for key in spec:
            # if values are types or tuple of types, convert to TypeSpec
            if isinstance(spec[key], (type, tuple)):
                spec[key] = TypeSpec(spec[key])
        
        return spec
    
    # otherwise, assume spec is already in canonical format
    return spec

def _should_validate(cls_instance, method, cache: bool = False):
    return not cache or method.__name__ not in cls_instance.__checked_input_specs_cache__

def _validate(cls_instance, method, data, spec, tag="data"):
    is_mapping = isinstance(spec, ModelSpec)
    is_tensor = isinstance(spec, TensorSpec)
    # use cls_instance to infer cache from
    cache_miss = _should_validate(cls_instance, method, cache=cache)

    if is_mapping:
        if not isinstance(data, Mapping):
            raise ValueError(
                f"{tag} must be a Mapping, got {type(data).__name__}"
            )
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

        if not (is_tensor or is_mapping):
            if not isinstance(data, spec):
                raise ValueError(
                    f"Input spec validation failed on "
                    f"{cls_instance.__class__.__name__}.{method.__name__}, "
                    f"expected {spec.__name__}, got "
                    f"{type(data).__name__}."
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
                    self,
                    func,
                    input_data,
                    input_spec_,
                    tag="input_data",
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
                _validate(
                    self,
                    func,
                    output_data,
                    output_spec_,
                    cache=cache,
                    tag="output_data",
                )

            if cache and func.__name__ not in self.__checked_output_specs_cache__:
                self.__checked_output_specs_cache__[func.__name__] = True

            return output_data
            

        wrapper.__checked_output_specs__ = True
        return wrapper

    return decorator

