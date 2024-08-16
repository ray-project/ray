from typing import Any, Dict

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

DATA_TYPE = Dict[str, Any]

IS_NOT_PROPERTY = "Spec {} must be a property of the class {}."


@ExperimentalAPI
class SpecDict(dict, Spec):
    """A dict containing `TensorSpec` and `Types`.

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
        check = self.is_subset(self, data, exact_match)
        if not check[0]:
            # Collect all (nested) keys in `data`.
            data_keys_set = set()

            def _map(path, s):
                data_keys_set.add(force_tuple(path))

            tree.map_structure_with_path(_map, data)

            raise ValueError(_MISSING_KEYS_FROM_DATA.format(check[1], data_keys_set))

    @staticmethod
    def is_subset(spec_dict, data_dict, exact_match=False):
        """Whether `spec_dict` is a subset of `data_dict`."""

        if exact_match:
            tree.assert_same_structure(data_dict, spec_dict, check_types=False)

        for key in spec_dict:
            # A key of `spec_dict` cannot be found in `data_dict` -> `spec_dict` is not
            # a subset.
            if key not in data_dict:
                return False, key
            # `data_dict` has same key.

            # `spec_dict`'s leaf value is None -> User does not want to specify
            # further -> continue.
            if spec_dict[key] is None:
                continue

            # `data_dict`'s leaf is another dict.
            elif isinstance(data_dict[key], dict):
                # `spec_dict`'s leaf is NOT another dict -> No match, return False and
                # the unmatched key.
                if not isinstance(spec_dict[key], dict):
                    return False, key

                # `spec_dict`'s leaf is another dict -> Recurse.
                res = SpecDict.is_subset(spec_dict[key], data_dict[key], exact_match)
                if not res[0]:
                    return res

            # `spec_dict`'s leaf is a dict (`data_dict`'s is not) -> No match, return
            # False and the unmatched key.
            elif isinstance(spec_dict[key], dict):
                return False, key

            # Neither `spec_dict`'s leaf nor `data_dict`'s leaf are dicts (and not None)
            # -> Compare spec with data.
            elif isinstance(spec_dict[key], Spec):
                try:
                    spec_dict[key].validate(data_dict[key])
                except ValueError as e:
                    raise ValueError(
                        f"Mismatch found in data element {key}, "
                        f"which is a TensorSpec: {e}"
                    )
            elif isinstance(spec_dict[key], (type, tuple)):
                if not isinstance(data_dict[key], spec_dict[key]):
                    raise ValueError(
                        _TYPE_MISMATCH.format(
                            key,
                            type(data_dict[key]).__name__,
                            spec_dict[key].__name__,
                        )
                    )
            else:
                raise ValueError(
                    f"The spec type has to be either TensorSpec or Type. "
                    f"got {type(spec_dict[key])}"
                )

        return True, None

    def __repr__(self) -> str:
        return f"SpecDict({repr(self.keys())})"
