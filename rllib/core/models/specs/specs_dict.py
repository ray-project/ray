from typing import Any, Dict

import tree
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.utils import force_tuple


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


class SpecDict(dict, Spec):
    def validate(
        self,
        data: DATA_TYPE,
        exact_match: bool = False,
    ) -> None:
        check = self.is_subset(self, data, exact_match)
        if not check[0]:
            data_keys_set = set()

            def _map(path, s):
                data_keys_set.add(force_tuple(path))

            tree.map_structure_with_path(_map, data)

            raise ValueError(_MISSING_KEYS_FROM_DATA.format(check[1], data_keys_set))

    @staticmethod
    def is_subset(spec_dict, data_dict, exact_match=False):
        if exact_match:
            tree.assert_same_structure(data_dict, spec_dict, check_types=False)

        for key in spec_dict:
            if key not in data_dict:
                return False, key
            if spec_dict[key] is None:
                continue

            elif isinstance(data_dict[key], dict):
                if not isinstance(spec_dict[key], dict):
                    return False, key

                res = SpecDict.is_subset(spec_dict[key], data_dict[key], exact_match)
                if not res[0]:
                    return res

            elif isinstance(spec_dict[key], dict):
                return False, key

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
