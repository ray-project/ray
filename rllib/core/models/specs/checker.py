import functools
import logging
from collections import abc
from typing import Union, Mapping, Any, Callable

from ray.rllib.core.models.specs.specs_base import Spec, TypeSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.utils.nested_dict import NestedDict
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class SpecCheckingError(Exception):
    """Raised when there is an error in the spec checking.

    This Error is raised when inputs or outputs do match the defined specs.
    """


@DeveloperAPI
def convert_to_canonical_format(spec: SpecType) -> Union[Spec, SpecDict]:
    """Converts a spec type input to the canonical format.

    The canonical format is either

    1. A nested SpecDict when the input spec is dict-like tree of specs and types or
    nested list of nested_keys.
    2. A single SpecType object, if the spec is a single constraint.

    The input can be any of the following:
        - a list of nested_keys. nested_keys are either strings or tuples of strings
        specifying the path to a leaf in the tree.
        - a tree of constraints. The tree structure can be specified as any nested
        hash-map structure (e.g. dict, SpecDict, NestedDict, etc.) The leaves of the
        tree can be either a Spec object, a type, or None. If the leaf is a type, it is
        converted to a TypeSpec. If the leaf is None, only the existance of the key is
        checked and the value will be None in the canonical format.
        - a single constraint. The constraint can be a Spec object, a type, or None.


    Examples of canoncial format #1:

    .. code-block:: python
        spec = ["foo", ("bar", "baz")]
        output = convert_to_canonical_format(spec)
        # output = SpecDict({"foo": None, ("bar", "baz"): None})

        spec = {"foo": int, "bar": {"baz": None}}
        output = convert_to_canonical_format(spec)
        # output = SpecDict(
        #   {"foo": TypeSpec(int), "bar": SpecDict({"baz": None})}
        # )

        spec = {"foo": int, "bar": {"baz": str}}
        output = convert_to_canonical_format(spec)
        # output = SpecDict(
        #   {"foo": TypeSpec(int), "bar": SpecDict({"baz": TypeSpec(str)})}
        # )

        spec = {"foo": int, "bar": {"baz": TensorSpec("b,h", framework="torch")}}
        output = convert_to_canonical_format(spec)
        # output = SpecDict(
        #   {"foo": TypeSpec(int), "bar": SpecDict({"baz": TensorSpec("b,h",
        framework="torch")})}
        # )


    # Example of canoncial format #2:

    .. code-block:: python
        spec = int
        output = convert_to_canonical_format(spec)
        # output = TypeSpec(int)

        spec = None
        output = convert_to_canonical_format(spec)
        # output = None

        spec = TensorSpec("b,h", framework="torch")
        output = convert_to_canonical_format(spec)
        # output = TensorSpec("b,h", framework="torch")

    Args:
        spec: The spec to convert to canonical format.

    Returns:
        The canonical format of the spec.
    """
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
            elif isinstance(spec[key], list):
                # this enables nested conversion of none-canonical formats
                spec[key] = convert_to_canonical_format(spec[key])
        return spec

    if isinstance(spec, type):
        return TypeSpec(spec)

    # otherwise, assume spec is already in canonical format
    return spec


def _should_validate(
    cls_instance: object, method: Callable, tag: str = "input"
) -> bool:
    """Returns True if the spec should be validated, False otherwise.

    The spec should be validated if the method is not cached (i.e. there is no cache
    storage attribute in the instance) or if the method is already cached. (i.e. it
    exists in the cache storage attribute)

    Args:
        cls_instance: The class instance that the method belongs to.
        method: The method to apply the spec checking to.
        tag: The tag of the spec to check. Either "input" or "output". This is used
        internally to defined an internal cache storage attribute based on the tag.

    Returns:
        True if the spec should be validated, False otherwise.
    """
    cache_store = getattr(cls_instance, f"__checked_{tag}_specs_cache__", None)
    return cache_store is None or method.__name__ not in cache_store


def _validate(
    *,
    cls_instance: object,
    method: Callable,
    data: Mapping[str, Any],
    spec: Spec,
    filter: bool = False,
    tag: str = "input",
) -> NestedDict:
    """Validate the data against the spec.

    Args:
        cls_instance: The class instance that the method belongs to.
        method: The method to apply the spec checking to.
        data: The data to validate.
        spec: The spec to validate against.
        filter: If True, the data will be filtered to only include the keys that are
            specified in the spec.
        tag: The tag of the spec to check. Either "input" or "output". This is used
            internally to defined an internal cache storage attribute based on the tag.

    Returns:
        The data, filtered if filter is True.
    """
    cache_miss = _should_validate(cls_instance, method, tag=tag)

    if isinstance(spec, SpecDict):
        if not isinstance(data, abc.Mapping):
            raise ValueError(f"{tag} must be a Mapping, got {type(data).__name__}")
        if cache_miss or filter:
            data = NestedDict(data)

    if cache_miss:
        try:
            spec.validate(data)
        except ValueError as e:
            raise SpecCheckingError(
                f"{tag} spec validation failed on "
                f"{cls_instance.__class__.__name__}.{method.__name__}, {e}."
            )

    return data


@DeveloperAPI(stability="alpha")
def check_input_specs(
    input_specs: str,
    *,
    only_check_on_retry: bool = True,
    filter: bool = False,
    cache: bool = False,
):
    """A general-purpose spec checker decorator for neural network base classes.

    This is a stateful decorator
    (https://realpython.com/primer-on-python-decorators/#stateful-decorators) to
    enforce input specs for any instance method that has an argument named
    `input_data` in its args.

    It also allows you to filter the input data dictionary to only include those keys
    that are specified in the model specs. It also allows you to cache the validation
    to make sure the spec is only validated once in the entire lifetime of the instance.

    Examples (See more examples in ../tests/test_specs_dict.py):

        >>> class MyModel(nn.Module):
        ...     @property
        ...     def input_specs(self):
        ...         return {"obs": TensorSpec("b, d", d=64)}
        ...
        ...     @check_input_specs("input_specs")
        ...     def forward(self, input_data, return_loss=False):
        ...         ...

        >>> model = MyModel()
        >>> model.forward({"obs": torch.randn(32, 64)}) # No error
        >>> model.forward({"obs": torch.randn(32, 32)}) # raises ValueError

    Args:
        func: The instance method to decorate. It should be a callable that takes
            `self` as the first argument, `input_data` as the second argument and any
            other keyword argument thereafter.
        input_specs: `self` should have an instance attribute whose name matches the
            string in input_specs and returns the `SpecDict`, `Spec`, or simply the
            `Type` that the `input_data` should comply with. It can also be None or
            empty list / dict to enforce no input spec.
        only_check_on_retry: If True, the spec will not be checked. Only if the
            decorated method raises an Exception, we check the spec to provide a more
            informative error message.
        filter: If True, and `input_data` is a nested dict the `input_data` will be
            filtered by its corresponding spec tree structure and then passed into the
            implemented function to make sure user is not confounded with unnecessary
            data.
        cache: If True, only checks the data validation for the first time the
            instance method is called.

    Returns:
        A wrapped instance method. In case of `cache=True`, after the first invokation
        of the decorated method, the intance will have `__checked_input_specs_cache__`
        attribute that stores which method has been invoked at least once. This is a
        special attribute that can be used for the cache itself. The wrapped class
        method also has a special attribute `__checked_input_specs__` that marks the
        method as decorated.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, input_data, **kwargs):
            if cache and not hasattr(self, "__checked_input_specs_cache__"):
                self.__checked_input_specs_cache__ = {}
            if cache and func.__name__ not in self.__checked_input_specs_cache__:
                self.__checked_input_specs_cache__[func.__name__] = True

            initial_exception = None
            if only_check_on_retry:
                # Attempt to run the function without spec checking
                try:
                    return func(self, input_data, **kwargs)
                except SpecCheckingError as e:
                    raise e
                except Exception as e:
                    # We store the initial exception to raise it later if the spec
                    # check fails.
                    initial_exception = e
                    logger.error(
                        f"Exception {e} raised on function call without checkin "
                        f"input specs. RLlib will now attempt to check the spec "
                        f"before calling the function again."
                    )

            # If the function was not executed successfully yet, we check specs
            checked_data = input_data
            if input_specs:
                if hasattr(self, input_specs):
                    spec = getattr(self, input_specs)
                else:
                    raise SpecCheckingError(
                        f"object {self} has no attribute {input_specs}."
                    )

                if spec is not None:
                    spec = convert_to_canonical_format(spec)
                    checked_data = _validate(
                        cls_instance=self,
                        method=func,
                        data=input_data,
                        spec=spec,
                        filter=filter,
                        tag="input",
                    )

                    if filter and isinstance(checked_data, NestedDict):
                        # filtering should happen regardless of cache
                        checked_data = checked_data.filter(spec)

            # If we have encountered an exception from calling `func` already,
            # we raise it again here and don't need to call func again.
            if initial_exception:
                raise initial_exception

            return func(self, checked_data, **kwargs)

        wrapper.__checked_input_specs__ = True
        return wrapper

    return decorator


@DeveloperAPI(stability="alpha")
def check_output_specs(
    output_specs: str,
    *,
    cache: bool = False,
):
    """A general-purpose spec checker decorator for Neural Network base classes.

    This is a stateful decorator
    (https://realpython.com/primer-on-python-decorators/#stateful-decorators) to
    enforce output specs for any instance method that outputs a single dict-like object.

    It also allows you to cache the validation to make sure the spec is only validated
    once in the entire lifetime of the instance.

    Examples (See more examples in ../tests/test_specs_dict.py):

        >>> class MyModel(nn.Module):
        ...     @property
        ...     def output_specs(self):
        ...         return {"obs": TensorSpec("b, d", d=64)}
        ...
        ...     @check_output_specs("output_specs")
        ...     def forward(self, input_data, return_loss=False):
        ...         return {"obs": torch.randn(32, 64)}

    Args:
        func: The instance method to decorate. It should be a callable that takes
            `self` as the first argument, `input_data` as the second argument and any
            other keyword argument thereafter. It should return a single dict-like
            object (i.e. not a tuple).
        output_specs: `self` should have an instance attribute whose name matches the
            string in output_specs and returns the `SpecDict`, `Spec`, or simply the
            `Type` that the `input_data` should comply with. It can alos be None or
            empty list / dict to enforce no input spec.
        cache: If True, only checks the data validation for the first time the
            instance method is called.

    Returns:
        A wrapped instance method. In case of `cache=True`, after the first invokation
        of the decorated method, the intance will have `__checked_output_specs_cache__`
        attribute that stores which method has been invoked at least once. This is a
        special attribute that can be used for the cache itself. The wrapped class
        method also has a special attribute `__checked_output_specs__` that marks the
        method as decorated.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, input_data, **kwargs):
            if cache and not hasattr(self, "__checked_output_specs_cache__"):
                self.__checked_output_specs_cache__ = {}

            output_data = func(self, input_data, **kwargs)

            if output_specs:
                if hasattr(self, output_specs):
                    spec = getattr(self, output_specs)
                else:
                    raise ValueError(f"object {self} has no attribute {output_specs}.")

                if spec is not None:
                    spec = convert_to_canonical_format(spec)
                    _validate(
                        cls_instance=self,
                        method=func,
                        data=output_data,
                        spec=spec,
                        tag="output",
                    )

            if cache and func.__name__ not in self.__checked_output_specs_cache__:
                self.__checked_output_specs_cache__[func.__name__] = True

            return output_data

        wrapper.__checked_output_specs__ = True
        return wrapper

    return decorator


@DeveloperAPI
def is_input_decorated(obj: object) -> bool:
    """Returns True if the object is decorated with `check_input_specs`."""
    return hasattr(obj, "__checked_input_specs__")


@DeveloperAPI
def is_output_decorated(obj: object) -> bool:
    """Returns True if the object is decorated with `check_output_specs`."""
    return hasattr(obj, "__checked_output_specs__")
