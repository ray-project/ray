"""Test utils in rllib/utils/space_utils.py."""

import unittest

import numpy as np
from gym.spaces import Box, Discrete, MultiDiscrete, MultiBinary, Tuple, Dict
from ray.rllib.utils.spaces.space_utils import convert_element_to_space_type


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

        box_space_uncoverted = box_space.sample().astype(np.float64)
        multi_discrete_unconverted = multi_discrete_space.sample().astype(np.int32)
        multi_binary_unconverted = multi_binary_space.sample().astype(np.int32)
        tuple_unconverted = (box_space_uncoverted, float(0))
        modified_element = {
            "box": box_space_uncoverted,
            "discrete": float(0),
            "multi_discrete": multi_discrete_unconverted,
            "multi_binary": multi_binary_unconverted,
            "tuple_space": tuple_unconverted,
            "dict_space": {
                "box2": box_space_uncoverted,
                "discrete2": float(0),
            },
        }
        element_with_correct_types = convert_element_to_space_type(
            modified_element, dict_space.sample()
        )
        assert dict_space.contains(element_with_correct_types)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
