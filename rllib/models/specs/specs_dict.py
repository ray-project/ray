from typing import Union, Mapping, Any

from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_base import Spec


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

DATA_TYPE = Union[NestedDict[Any], Mapping[str, Any]]

IS_NOT_PROPERTY = "Spec {} must be a property of the class {}."


@ExperimentalAPI
class SpecDict(NestedDict[Spec], Spec):
    """A NestedDict containing `TensorSpec` and `Types`.

    It can be used to validate an incoming data against a nested dictionary of specs.

    Examples:

        Basic validation:
        -----------------
        >>> spec_dict = SpecDict({
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

    @override(Spec)
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
            if spec is None:
                continue

            if isinstance(spec, Spec):
                try:
                    spec.validate(data_to_validate)
                except ValueError as e:
                    raise ValueError(
                        f"Mismatch found in data element {spec_name}, "
                        f"which is a TensorSpec: {e}"
                    )
            elif isinstance(spec, (type, tuple)):
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
        return f"SpecDict({repr(self._data)})"
