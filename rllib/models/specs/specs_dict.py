import functools
from typing import Union, Type, Mapping, Any

from ray.util.annotations import PublicAPI, DeveloperAPI

from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_base import TensorSpec


_MISSING_KEYS_FROM_SPEC = (
    "The data dict does not match the model specs. Keys {} are "
    "in the data dict but not on the given spec dict, and exact_match is set to True"
)
_MISSING_KEYS_FROM_DATA = (
    "The data dict does not match the model specs. Keys {} are "
    "in the spec dict but not on the data dict. Data keys are {}"
)
_TYPE_MISMATCH = (
    "The data does not match the spec. The data element "
    "{} has type {} (expected type {})."
)

SPEC_LEAF_TYPE = Union[Type, TensorSpec]
DATA_TYPE = Union[NestedDict[Any], Mapping[str, Any]]

IS_NOT_PROPERTY = "Spec {} must be a property of the class {}."


@PublicAPI(stability="alpha")
class ModelSpec(NestedDict[SPEC_LEAF_TYPE]):
    """A NestedDict containing `TensorSpec` and `Types`.

    It can be used to validate an incoming data against a nested dictionary of specs.

    Examples:

        Basic validation:
        -----------------
        >>> spec_dict = ModelSpec({
        ...     "obs": {
        ...         "arm":      TensorSpec("b, dim_arm", dim_arm=64),
        ...         "gripper":  TensorSpec("b, dim_grip", dim_grip=12)
        ...     },
        ...     "action": TensorSpec("b, dim_action", dim_action=12),
        ...     "action_dist": torch.distributions.Categorical
        ... })

        >>> spec_dict.validate({
        ...     "obs": {
        ...         "arm":      torch.randn(32, 64),
        ...         "gripper":  torch.randn(32, 12)
        ...     },
        ...     "action": torch.randn(32, 12),
        ...     "action_dist": torch.distributions.Categorical(torch.randn(32, 12))
        ... }) # No error

        >>> spec_dict.validate({
        ...     "obs": {
        ...         "arm":      torch.randn(32, 32), # Wrong shape
        ...         "gripper":  torch.randn(32, 12)
        ...     },
        ...     "action": torch.randn(32, 12),
        ...     "action_dist": torch.distributions.Categorical(torch.randn(32, 12))
        ... }) # raises ValueError

        Filtering input data:
        ---------------------
        >>> input_data = {
        ...     "obs": {
        ...         "arm":      torch.randn(32, 64),
        ...         "gripper":  torch.randn(32, 12),
        ...         "unused":   torch.randn(32, 12)
        ...     },
        ...     "action": torch.randn(32, 12),
        ...     "action_dist": torch.distributions.Categorical(torch.randn(32, 12)),
        ...     "unused": torch.randn(32, 12)
        ... }
        >>> input_data.filter(spec_dict) # returns a dict with only the keys in the spec
        {
            "obs": {
                "arm":      input_data["obs"]["arm"],
                "gripper":  input_data["obs"]["gripper"]
            },
            "action": input_data["action"],
            "action_dist": input_data["action_dist"]
        }

    Raises:
        ValueError: If the data doesn't match the spec.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._keys_set = set(self.keys())

    def validate(
        self,
        data: DATA_TYPE,
        exact_match: bool = False,
    ) -> None:
        """Checks whether the data matches the spec.

        Args:
            data: The data which should match the spec. It can also be a spec.
            exact_match: If true, the data and the spec must be exactly identical.
                Otherwise, the data is validated as long as it contains at least the
                elements of the spec, but can contain more entries.
        Raises:
            ValueError: If the data doesn't match the spec.
        """
        data = NestedDict(data)
        data_keys_set = set(data.keys())
        missing_keys = self._keys_set.difference(data_keys_set)
        if missing_keys:
            raise ValueError(
                _MISSING_KEYS_FROM_DATA.format(missing_keys, data_keys_set)
            )
        if exact_match:
            data_spec_missing_keys = data_keys_set.difference(self._keys_set)
            if data_spec_missing_keys:
                raise ValueError(_MISSING_KEYS_FROM_SPEC.format(data_spec_missing_keys))

        for spec_name, spec in self.items():
            data_to_validate = data[spec_name]
            if isinstance(spec, TensorSpec):
                try:
                    spec.validate(data_to_validate)
                except ValueError as e:
                    raise ValueError(
                        f"Mismatch found in data element {spec_name}, "
                        f"which is a TensorSpec: {e}"
                    )
            elif isinstance(spec, (Type, tuple)):
                if not isinstance(data_to_validate, spec):
                    raise ValueError(
                        _TYPE_MISMATCH.format(
                            spec_name, type(data_to_validate).__name__, spec.__name__
                        )
                    )
            else:
                raise ValueError(
                    f"The spec type has to be either TensorSpec or Type. "
                    f"got {type(spec)}"
                )

    @override(NestedDict)
    def __repr__(self) -> str:
        return f"ModelSpec({repr(self._data)})"


@DeveloperAPI
def check_specs(
    input_spec: str = "",
    output_spec: str = "",
    filter: bool = False,
    cache: bool = True,
    input_exact_match: bool = False,
    output_exact_match: bool = False,
):
    """A general-purpose check_specs decorator for Neural Network modules.

    This is a stateful decorator
    (https://realpython.com/primer-on-python-decorators/#stateful-decorators) to
    enforce input/output specs for any instance method that has an argument named
    `input_data` in its args and returns a single object.

    It also allows you to filter the input data dictionary to only include those keys
    that are specified in the model specs. It also allows you to cache the validation
    to make sure the spec is only validated once in the entire lifetime of the instance.

    Examples (See more exmaples in ../tests/test_specs_dict.py):

        >>> class MyModel(nn.Module):
        ...     def input_spec(self):
        ...         return ModelSpec({"obs": TensorSpec("b, d", d=64)})
        ...
        ...     @check_specs(input_spec="input_spec")
        ...     def forward(self, input_data, return_loss=False):
        ...         ...
        ...         output_dict = ...
        ...         return output_dict

        >>> model = MyModel()
        >>> model.forward({"obs": torch.randn(32, 64)}) # No error
        >>> model.forward({"obs": torch.randn(32, 32)}) # raises ValueError

    Args:
        func: The instance method to decorate. It should be a callable that takes
            `self` as the first argument, `input_data` as the second argument and any
            other keyword argument thereafter. It should return a single object
            (i.e. not a tuple).
        input_spec: `self` should have an instance method whose name matches the string
            in input_spec and returns the `ModelSpec`, `TensorSpec`, or simply the
            `Type` that the `input_data` should comply with.
        output_spec: `self` should have an instance method whose name matches the
            string in output_spec and returns the spec that the output should comply
            with.
        filter: If True, and `input_data` is a nested dict the `input_data` will be
            filtered by its corresponding spec tree structure and then passed into the
            implemented function to make sure user is not confounded with unnecessary
            data.
        cache: If True, only checks the input/output validation for the first time the
            instance method is called.
        input_exact_match: If True, the input data (should be a nested dict) must match
            the spec exactly. Otherwise, the data is validated as long as it contains
            at least the elements of the spec, but can contain more entries.
        output_exact_match: If True, the output data (should be a nested dict) must
            match the spec exactly. Otherwise, the data is validated as long as it
            contains at least the elements of the spec, but can contain more entries.

    Returns:
        A wrapped instance method. In case of `cache=True`, after the first invokation
        of the decorated method, the intance will have `__checked_specs_cache__`
        attribute that store which method has been invoked at least once. This is a
        special attribute that can be used for the cache itself. The wrapped class
        method also has a special attribute `__checked_specs__` that marks the method as
        decorated.
    """

    if not input_spec and not output_spec:
        raise ValueError("At least one of input_spec or output_spec must be provided.")

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, input_data, **kwargs):
            if cache and not hasattr(self, "__checked_specs_cache__"):
                self.__checked_specs_cache__ = {}

            def should_validate():
                return not cache or func.__name__ not in self.__checked_specs_cache__

            def validate(data, spec, exact_match, tag="data"):
                is_mapping = isinstance(spec, ModelSpec)
                is_tensor = isinstance(spec, TensorSpec)
                cache_miss = should_validate()

                if is_mapping:
                    if not isinstance(data, Mapping):
                        raise ValueError(
                            f"{tag} must be a Mapping, got {type(data).__name__}"
                        )
                    if cache_miss or filter:
                        data = NestedDict(data)

                if cache_miss:
                    try:
                        if is_mapping:
                            spec.validate(data, exact_match=exact_match)
                        elif is_tensor:
                            spec.validate(data)
                    except ValueError as e:
                        raise ValueError(
                            f"{tag} spec validation failed on "
                            f"{self.__class__.__name__}.{func.__name__}, {e}."
                        )

                    if not (is_tensor or is_mapping):
                        if not isinstance(data, spec):
                            raise ValueError(
                                f"Input spec validation failed on "
                                f"{self.__class__.__name__}.{func.__name__}, "
                                f"expected {spec.__name__}, got "
                                f"{type(data).__name__}."
                            )
                return data

            input_data_ = input_data
            if input_spec:
                input_spec_ = getattr(self, input_spec)

                input_data_ = validate(
                    input_data,
                    input_spec_,
                    exact_match=input_exact_match,
                    tag="input_data",
                )

                if filter and isinstance(input_spec_, (ModelSpec, TensorSpec)):
                    # filtering should happen regardless of cache
                    input_data_ = input_data_.filter(input_spec_)

            output_data = func(self, input_data_, **kwargs)
            if output_spec:
                output_spec_ = getattr(self, output_spec)
                validate(
                    output_data,
                    output_spec_,
                    exact_match=output_exact_match,
                    tag="output_data",
                )

            if cache and func.__name__ not in self.__checked_specs_cache__:
                self.__checked_specs_cache__[func.__name__] = True

            return output_data

        wrapper.__checked_specs__ = True
        return wrapper

    return decorator
