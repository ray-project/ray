import unittest

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.utils.tf_utils import get_placeholder


class TestTfUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_get_placeholder_w_discrete(self):
        """Tests whether `get_placeholder` works as expected on Box spaces."""
        space = gym.spaces.Discrete(2)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=False, one_hot=False
        )
        self.assertTrue(placeholder.shape.as_list() == [None])
        self.assertTrue(placeholder.dtype.name == "int64")

        space = gym.spaces.Discrete(3)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=True, one_hot=False
        )
        self.assertTrue(placeholder.shape.as_list() == [None])
        self.assertTrue(placeholder.dtype.name == "int64")

        space = gym.spaces.Discrete(4)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=False, one_hot=True
        )
        self.assertTrue(placeholder.shape.as_list() == [None, 4])
        self.assertTrue(placeholder.dtype.name == "float32")

    def test_get_placeholder_w_box(self):
        """Tests whether `get_placeholder` works as expected on Box spaces."""
        space = gym.spaces.Box(-1.0, 1.0, (2,), dtype=np.float32)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=False, one_hot=False
        )
        self.assertTrue(placeholder.shape.as_list() == [None, 2])
        self.assertTrue(placeholder.dtype.name == "float32")

        space = gym.spaces.Box(-1.0, 1.0, (2, 3), dtype=np.float32)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=False, one_hot=False
        )
        self.assertTrue(placeholder.shape.as_list() == [None, 2, 3])
        self.assertTrue(placeholder.dtype.name == "float32")

        space = gym.spaces.Box(-1.0, 1.0, (2, 3), dtype=np.float32)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=True, one_hot=False
        )
        self.assertTrue(placeholder.shape.as_list() == [None, 2, 3])
        self.assertTrue(placeholder.dtype.name == "float32")

        space = gym.spaces.Box(-1.0, 1.0, (2, 3), dtype=np.float32)
        placeholder = get_placeholder(
            space=space, time_axis=False, flatten=True, one_hot=True
        )
        self.assertTrue(placeholder.shape.as_list() == [None, 2, 3])
        self.assertTrue(placeholder.dtype.name == "float32")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
