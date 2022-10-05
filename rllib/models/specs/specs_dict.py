from typing import Union, Type, Mapping, Any

from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_base import TensorSpecs


_MISSING_KEYS_FROM_SPEC = (
    "The data dict does not match the model specs. Keys {} are "
    "in the data dict but not on the given spec dict, and exact_match is set to True."
)
_MISSING_KEYS_FROM_DATA = (
    "The data dict does not match the model specs. Keys {} are "
    "in the spec dict but not on the data dict."
)
_TYPE_MISMATCH = (
    "The data does not match the spec. The data element "
    "{} has type {} (expected type {})."
)

SPEC_LEAF_TYPE = Union[Type, TensorSpecs]
DATA_TYPE = Union[NestedDict[Any], Mapping[str, Any]]


class ModelSpecDict(NestedDict[SPEC_LEAF_TYPE]):
    """A NestedDict containing specs and class types."""

    def validate(
        self,
        data: DATA_TYPE,
        exact_match: bool = True,
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
        missing_keys = set(self.keys()).difference(set(data.keys()))
        if missing_keys:
            raise ValueError(_MISSING_KEYS_FROM_DATA.format(missing_keys))
        if exact_match:
            data_spec_missing_keys = set(data.keys()).difference(set(self.keys()))
            if data_spec_missing_keys:
                raise ValueError(_MISSING_KEYS_FROM_SPEC.format(data_spec_missing_keys))

        for spec_name, spec in self.items():
            data_to_validate = data[spec_name]
            if isinstance(spec, TensorSpecs):
                spec.validate(data_to_validate)
            elif isinstance(spec, Type):
                if not isinstance(data_to_validate, spec):
                    raise ValueError(
                        _TYPE_MISMATCH.format(
                            spec_name, type(data_to_validate).__name__, spec.__name__
                        )
                    )

    @override(NestedDict)
    def __repr__(self) -> str:
        return f"ModelSpecDict({repr(self._data)})"
