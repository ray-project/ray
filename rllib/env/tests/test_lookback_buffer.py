import unittest

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.env.utils import LookbackBuffer
from ray.rllib.utils.test_utils import check


class TestLookbackBuffer(unittest.TestCase):
    space = gym.spaces.Dict({
        "a": gym.spaces.Discrete(4),
        "b": gym.spaces.Box(-1.0, 1.0, (2, 3)),
        "c": gym.spaces.Tuple([
            gym.spaces.MultiDiscrete([2, 3]),
            gym.spaces.Box(-1.0, 1.0, (1,))
        ])
    })

    #@classmethod
    #def setUpClass(cls) -> None:
    #    ray.init()

    #@classmethod
    #def tearDownClass(cls) -> None:
    #    ray.shutdown()

    def test_adding(self):
        buffer = LookbackBuffer(data=[0, 1, 2, 3])
        self.assertTrue(len(buffer), 4)
        buffer.add(4)
        self.assertTrue(len(buffer), 5)
        buffer.add(5)
        self.assertTrue(len(buffer), 6)

    def test_complex_structs(self):
        buffer = LookbackBuffer(data=[
            self.space.sample() for _ in range(4)
        ])
        self.assertTrue(len(buffer), 4)
        buffer.add(self.space.sample())
        self.assertTrue(len(buffer), 5)
        buffer.add(self.space.sample())
        self.assertTrue(len(buffer), 6)
        buffer.finalize()
        self.assertRaises(RuntimeError, lambda: buffer.add("something"))
        self.assertTrue(isinstance(buffer.data, dict))
        self.assertTrue(isinstance(buffer.data["a"], np.ndarray))
        self.assertTrue(isinstance(buffer.data["b"], np.ndarray))
        self.assertTrue(isinstance(buffer.data["c"], tuple))
        self.assertTrue(isinstance(buffer.data["c"][0], np.ndarray))
        self.assertTrue(isinstance(buffer.data["c"][1], np.ndarray))

    def test_lookback(self):
        buffer = LookbackBuffer(data=[0, 1, 2, 3], lookback=2)
        self.assertTrue(len(buffer), 2)
        data_no_lookback = buffer.get()
        check(data_no_lookback, [2, 3])
        buffer.add(4)
        self.assertTrue(len(buffer), 3)
        buffer.add(5)
        self.assertTrue(len(buffer), 4)
        data_no_lookback = buffer.get()
        check(data_no_lookback, [2, 3, 4, 5])

    def test_get_with_lookback_simple(self):
        buffer = LookbackBuffer(data=[0, 1, 2, 3, 4], lookback=2)
        self.assertTrue(len(buffer), 3)

        # No args: Expect all contents excluding lookback buffer.
        check(buffer.get(), [2, 3, 4])
        # Individual negative indices (include lookback buffer).
        check(buffer.get(-1), 4)
        check(buffer.get(-2), 3)
        check(buffer.get(-4), 1)
        check(buffer.get([-4]), [1])
        # Individual positive indices (do NOT include lookback buffer).
        check(buffer.get(0), 2)
        check(buffer.get(1), 3)
        check(buffer.get(2), 4)
        check(buffer.get([2]), [4])
        # List of negative indices (include lookback buffer).
        check(buffer.get([-4, -5]), [1, 0])
        check(buffer.get(-1), 4)
        check(buffer.get([-1]), [4])
        # List of positive indices (do NOT include lookback buffer).
        check(buffer.get([1, 0, 2]), [3, 2, 4])
        # Slices.
        # Type: [(None|0):...]
        check(buffer.get(slice(None, None)), [2, 3, 4])
        check(buffer.get(slice(None, 2)), [2, 3])
        check(buffer.get(slice(0, 2)), [2, 3])
        check(buffer.get(slice(3)), [2, 3, 4])
        check(buffer.get(slice(None, -1)), [2, 3])
        check(buffer.get(slice(0, -1)), [2, 3])
        check(buffer.get(slice(None, -2)), [2])
        check(buffer.get(slice(0, -2)), [2])
        # Type: [...:None]
        check(buffer.get(slice(2, None)), [4])
        check(buffer.get(slice(2, 5)), [4])
        check(buffer.get(slice(1, None)), [3, 4])
        check(buffer.get(slice(1, 5)), [3, 4])
        check(buffer.get(slice(-1, None)), [4])
        check(buffer.get(slice(-1, 5)), [4])
        check(buffer.get(slice(-4, None)), [1, 2, 3, 4])
        check(buffer.get(slice(-4, 5)), [1, 2, 3, 4])
        # Type: [-n:-m]
        check(buffer.get(slice(-2, -1)), [3])
        check(buffer.get(slice(-3, -1)), [2, 3])
        check(buffer.get(slice(-4, -2)), [1, 2])
        check(buffer.get(slice(-4, -1)), [1, 2, 3])
        check(buffer.get(slice(-5, -1)), [0, 1, 2, 3])
        check(buffer.get(slice(-6, -2)), [0, 1, 2])

        buffer.finalize()
        data = buffer.get([1, 0, 2])
        self.assertTrue(isinstance(data, np.ndarray))
        check(data, [3, 2, 4])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
