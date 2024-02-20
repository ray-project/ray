"""Test utils in rllib/utils/space_utils.py."""

import unittest

from gymnasium.spaces import Box, Discrete, MultiDiscrete, MultiBinary, Tuple, Dict
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.spaces.space_utils import (
    batch,
    convert_element_to_space_type,
    get_base_struct_from_space,
    unbatch,
    unsquash_action,
)
from ray.rllib.utils.test_utils import check


class TestSpaceUtils(unittest.TestCase):
    def test_convert_element_to_space_type(self):
        """Test if space converter works for all elements/space permutations"""
        box_space = Box(low=-1, high=1, shape=(2,))
        discrete_space = Discrete(2)
        multi_discrete_space = MultiDiscrete([2, 2])
        multi_binary_space = MultiBinary(2)
        tuple_space = Tuple((box_space, discrete_space))
        dict_space = Dict(
            {
                "box": box_space,
                "discrete": discrete_space,
                "multi_discrete": multi_discrete_space,
                "multi_binary": multi_binary_space,
                "dict_space": Dict(
                    {
                        "box2": box_space,
                        "discrete2": discrete_space,
                    }
                ),
                "tuple_space": tuple_space,
            }
        )

        box_space_unconverted = box_space.sample().astype(np.float64)
        multi_discrete_unconverted = multi_discrete_space.sample().astype(np.int32)
        multi_binary_unconverted = multi_binary_space.sample().astype(np.int32)
        tuple_unconverted = (box_space_unconverted, float(0))
        modified_element = {
            "box": box_space_unconverted,
            "discrete": float(0),
            "multi_discrete": multi_discrete_unconverted,
            "multi_binary": multi_binary_unconverted,
            "tuple_space": tuple_unconverted,
            "dict_space": {
                "box2": box_space_unconverted,
                "discrete2": float(0),
            },
        }
        element_with_correct_types = convert_element_to_space_type(
            modified_element, dict_space.sample()
        )
        assert dict_space.contains(element_with_correct_types)

    def test_unsquash_action(self):
        """Test to make sure unsquash_action works for both float and int Box spaces."""
        space = Box(low=3, high=8, shape=(2,), dtype=np.float32)
        struct = get_base_struct_from_space(space)
        action = unsquash_action(0.5, struct)
        self.assertEqual(action[0], 6.75)
        self.assertEqual(action[1], 6.75)

        space = Box(low=3, high=8, shape=(2,), dtype=np.int32)
        struct = get_base_struct_from_space(space)
        action = unsquash_action(3, struct)
        self.assertEqual(action[0], 6)
        self.assertEqual(action[1], 6)

    def test_batch_and_unbatch(self):
        """Tests the two utility functions `batch` and `unbatch`."""
        # Test, whether simple structs are batch/unbatch'able as well.
        # B=8
        simple_struct = [0, 1, 2, 3, 4, 5, 6, 7]
        simple_struct_batched = batch(simple_struct)
        check(unbatch(simple_struct_batched), simple_struct)

        # Create a complex struct of individual batches (B=2).
        complex_struct = {
            "a": (
                np.array([-10.0, -20.0]),
                {
                    "a1": np.array([-1, -2]),
                    "a2": np.array([False, False]),
                },
            ),
            "b": np.array([0, 1]),
            "c": {
                "c1": np.array([True, False]),
                "c2": np.array([1, 2]),
                "c3": (np.array([3, 4]), np.array([5, 6])),
            },
            "d": np.array([0.0, 0.1]),
        }
        complex_struct_unbatched = unbatch(complex_struct)
        # Check that we now have a list of two complex items, the first one
        # containing all the index=0 values, the second one containing all the index=1
        # values.
        check(
            complex_struct_unbatched,
            [
                tree.map_structure(lambda s: s[0], complex_struct),
                tree.map_structure(lambda s: s[1], complex_struct),
            ],
        )

        # Re-batch the unbatched struct.
        complex_struct_rebatched = batch(complex_struct_unbatched)
        # Should be identical to original struct.
        check(complex_struct, complex_struct_rebatched)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
