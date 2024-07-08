from typing import Any, Dict, Mapping

import tree
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils import force_tuple


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

DATA_TYPE = Mapping[str, Any]

IS_NOT_PROPERTY = "Spec {} must be a property of the class {}."


@ExperimentalAPI
class SpecDict(Dict[str, Spec], Spec):
    """A Dict containing `TensorSpec` and `Types`.

    It can be used to validate an incoming data against a nested dictionary of specs.

    Examples:

        Basic validation:
        -----------------

        .. testcode::
            :skipif: True

            spec_dict = SpecDict({
                "obs": {
                    "arm":      TensorSpec("b, dim_arm", dim_arm=64),
                    "gripper":  TensorSpec("b, dim_grip", dim_grip=12)
                },
                "action": TensorSpec("b, dim_action", dim_action=12),
                "action_dist": torch.distributions.Categorical

            spec_dict.validate({
                "obs": {
                    "arm":      torch.randn(32, 64),
                    "gripper":  torch.randn(32, 12)
                },
                "action": torch.randn(32, 12),
                "action_dist": torch.distributions.Categorical(torch.randn(32, 12))
            }) # No er
            spec_dict.validate({
                "obs": {
                    "arm":      torch.randn(32, 32), # Wrong shape
                    "gripper":  torch.randn(32, 12)
                },
                "action": torch.randn(32, 12),
                "action_dist": torch.distributions.Categorical(torch.randn(32, 12))
            }) # raises ValueError

        Filtering input data:
        ---------------------

        .. testcode::
            :skipif: True

            input_data = {
                "obs": {
                    "arm":      torch.randn(32, 64),
                    "gripper":  torch.randn(32, 12),
                    "unused":   torch.randn(32, 12)
                },
                "action": torch.randn(32, 12),
                "action_dist": torch.distributions.Categorical(torch.randn(32, 12)),
                "unused": torch.randn(32, 12)
            }
            input_data.filter(spec_dict) # returns a dict with only the keys in the spec

        .. testoutput::

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

    #def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)

        # Collect all (nested) keys in `self`.
        #_keys_set = set()

    #    def _map(path, s):
    #        _keys_set.add(force_tuple(path))

    #    tree.map_structure_with_path(_map, self)
    #    self._keys_set = _keys_set

        #self._keys_set = set(self.keys())

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
                Otherwise, the data is considered valid as long as it contains at least
                the elements of the spec, but can contain more entries.
        Raises:
            ValueError: If the data doesn't match the spec.
        """
        # Collect all (nested) keys in `data`.
        data_keys_set = set()

        def _map(path, s):
            data_keys_set.add(force_tuple(path))

        tree.map_structure_with_path(_map, data)

        # Check, whether all (nested) keys in `self` (the Spec) also exist in `data`.
        def _check(path, s):
            path = force_tuple(path)
            if s not in data_keys_set:
                raise ValueError(_MISSING_KEYS_FROM_DATA.format(path, data_keys_set))

        tree.map_structure_with_path(_check, self)

        if exact_match:
            tree.assert_same_structure(data, self, check_types=False)

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

    def __repr__(self) -> str:
        return f"SpecDict({repr(self.keys())})"
