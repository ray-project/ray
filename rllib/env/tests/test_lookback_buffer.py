import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.env.utils import BufferWithInfiniteLookback
from ray.rllib.utils.spaces.space_utils import batch, get_dummy_batch_for_space
from ray.rllib.utils.test_utils import check


class TestBufferWithInfiniteLookback(unittest.TestCase):
    space = gym.spaces.Dict(
        {
            "a": gym.spaces.Discrete(4),
            "b": gym.spaces.Box(-1.0, 1.0, (2, 3)),
            "c": gym.spaces.Tuple(
                [gym.spaces.MultiDiscrete([2, 3]), gym.spaces.Box(-1.0, 1.0, (1,))]
            ),
        }
    )

    def test_append_and_pop(self):
        buffer = BufferWithInfiniteLookback(data=[0, 1, 2, 3])
        self.assertTrue(len(buffer), 4)
        buffer.append(4)
        self.assertTrue(len(buffer), 5)
        buffer.append(5)
        self.assertTrue(len(buffer), 6)
        buffer.pop()
        self.assertTrue(len(buffer), 5)
        buffer.pop()
        self.assertTrue(len(buffer), 4)
        buffer.append(10)
        self.assertTrue(len(buffer), 5)
        check(buffer.data, [0, 1, 2, 3, 10])
        buffer.finalize()
        self.assertRaises(RuntimeError, lambda: buffer.append("something"))
        self.assertRaises(RuntimeError, lambda: buffer.extend(["something"]))
        self.assertRaises(RuntimeError, lambda: buffer.pop())

    def test_complex_structs(self):
        buffer = BufferWithInfiniteLookback(
            data=[self.space.sample() for _ in range(4)]
        )
        self.assertTrue(len(buffer), 4)
        buffer.append(self.space.sample())
        self.assertTrue(len(buffer), 5)
        buffer.append(self.space.sample())
        self.assertTrue(len(buffer), 6)

        buffer.finalize()
        self.assertRaises(RuntimeError, lambda: buffer.append("something"))

        self.assertTrue(isinstance(buffer.data, dict))
        self.assertTrue(isinstance(buffer.data["a"], np.ndarray))
        self.assertTrue(isinstance(buffer.data["b"], np.ndarray))
        self.assertTrue(isinstance(buffer.data["c"], tuple))
        self.assertTrue(isinstance(buffer.data["c"][0], np.ndarray))
        self.assertTrue(isinstance(buffer.data["c"][1], np.ndarray))

    def test_lookback(self):
        buffer = BufferWithInfiniteLookback(data=[0, 1, 2, 3], lookback=2)
        self.assertTrue(len(buffer), 2)
        data_no_lookback = buffer.get()
        check(data_no_lookback, [2, 3])
        buffer.append(4)
        self.assertTrue(len(buffer), 3)
        buffer.append(5)
        self.assertTrue(len(buffer), 4)
        data_no_lookback = buffer.get()
        check(data_no_lookback, [2, 3, 4, 5])
        buffer.pop()
        self.assertTrue(len(buffer), 3)
        data_no_lookback = buffer.get()
        check(data_no_lookback, [2, 3, 4])

    def test_get_with_lookback(self):
        """Tests `get` and `getitem` functionalities with a lookback range > 0."""
        buffer = BufferWithInfiniteLookback(data=[0, 1, 2, 3, 4], lookback=2)

        # Test on ongoing and finalized buffer.
        for finalized in [False, True]:
            if finalized:
                buffer.finalize()

            self.assertTrue(len(buffer), 3)
            # No args: Expect all contents excluding lookback buffer.
            check(buffer.get(), [2, 3, 4])
            check(buffer[:], [2, 3, 4])
            # Individual negative indices (include lookback buffer).
            check(buffer.get(-1), 4)
            check(buffer[-1], 4)
            check(buffer.get(-2), 3)
            check(buffer[-2], 3)
            check(buffer.get(-4), 1)
            check(buffer[-4], 1)
            check(buffer.get([-4]), [1])
            check(buffer[-4:-3], [1])
            self.assertRaises(IndexError, lambda: buffer.get(-6))
            self.assertRaises(IndexError, lambda: buffer[-6])
            self.assertRaises(IndexError, lambda: buffer.get(-1000))
            self.assertRaises(IndexError, lambda: buffer[-1000])
            # Individual positive indices (do NOT include lookback buffer).
            check(buffer.get(0), 2)
            check(buffer[0], 2)
            check(buffer.get(1), 3)
            check(buffer[1], 3)
            check(buffer.get(2), 4)
            check(buffer[2], 4)
            check(buffer.get([2]), [4])
            check(buffer[2:3], [4])
            self.assertRaises(IndexError, lambda: buffer.get(3))
            self.assertRaises(IndexError, lambda: buffer[3])
            self.assertRaises(IndexError, lambda: buffer.get(1000))
            self.assertRaises(IndexError, lambda: buffer[1000])
            # List of negative indices (include lookback buffer).
            check(buffer.get([-4, -5]), [1, 0])
            check(buffer.get([-1]), [4])
            check(buffer[-1:], [4])
            check(buffer.get([-5]), [0])
            check(buffer[-5:-4], [0])
            self.assertRaises(IndexError, lambda: buffer.get([-6]))
            self.assertRaises(IndexError, lambda: buffer.get([-1, -6]))
            self.assertRaises(IndexError, lambda: buffer.get([-1000]))
            # List of positive indices (do NOT include lookback buffer).
            check(buffer.get([1, 0, 2]), [3, 2, 4])
            check(buffer.get([0, 2, 1]), [2, 4, 3])
            self.assertRaises(IndexError, lambda: buffer.get([6]))
            self.assertRaises(IndexError, lambda: buffer.get([1, 6]))
            self.assertRaises(IndexError, lambda: buffer.get([1000]))
            # List of positive and negative indices (do NOT include lookback buffer).
            check(buffer.get([1, 0, -2]), [3, 2, 3])
            check(buffer.get([-3, 1, -1]), [2, 3, 4])
            # Slices.
            # Type: [None:...]
            check(buffer.get(slice(None, None)), [2, 3, 4])
            check(buffer.get(slice(None, 2)), [2, 3])
            check(buffer[:2], [2, 3])
            check(buffer.get(slice(3)), [2, 3, 4])
            check(buffer[:3], [2, 3, 4])
            check(buffer.get(slice(None, -1)), [2, 3])
            check(buffer[:-1], [2, 3])
            check(buffer.get(slice(None, -2)), [2])
            check(buffer[:-2], [2])
            # Type: [...:None]
            check(buffer.get(slice(2, None)), [4])
            check(buffer[2:], [4])
            check(buffer.get(slice(2, 5)), [4])
            check(buffer[2:5], [4])
            check(buffer.get(slice(1, None)), [3, 4])
            check(buffer[1:], [3, 4])
            check(buffer.get(slice(1, 5)), [3, 4])
            check(buffer[1:5], [3, 4])
            check(buffer.get(slice(-1, None)), [4])
            check(buffer[-1:], [4])
            check(buffer.get(slice(-1, 5)), [4])
            check(buffer[-1:5], [4])
            check(buffer.get(slice(-4, None)), [1, 2, 3, 4])
            check(buffer[-4:], [1, 2, 3, 4])
            check(buffer.get(slice(-4, 5)), [1, 2, 3, 4])
            check(buffer[-4:5], [1, 2, 3, 4])
            # Type: [-n:-m]
            check(buffer.get(slice(-2, -1)), [3])
            check(buffer[-2:-1], [3])
            check(buffer.get(slice(-3, -1)), [2, 3])
            check(buffer[-3:-1], [2, 3])
            check(buffer.get(slice(-4, -2)), [1, 2])
            check(buffer[-4:-2], [1, 2])
            check(buffer.get(slice(-4, -1)), [1, 2, 3])
            check(buffer[-4:-1], [1, 2, 3])
            check(buffer.get(slice(-5, -1)), [0, 1, 2, 3])
            check(buffer[-5:-1], [0, 1, 2, 3])
            check(buffer.get(slice(-6, -2)), [0, 1, 2])
            check(buffer[-6:-2], [0, 1, 2])
            # Type: [+n:+m]
            check(buffer.get(slice(0, 1)), [2])
            check(buffer[0:1], [2])
            check(buffer.get(slice(0, 2)), [2, 3])
            check(buffer[0:2], [2, 3])
            check(buffer.get(slice(0, 3)), [2, 3, 4])
            check(buffer[0:3], [2, 3, 4])
            check(buffer.get(slice(1, 2)), [3])
            check(buffer[1:2], [3])
            check(buffer.get(slice(1, 3)), [3, 4])
            check(buffer[1:3], [3, 4])
            check(buffer.get(slice(2, 3)), [4])
            check(buffer[2:3], [4])
            # Type: [+n:-m]
            check(buffer.get(slice(0, -1)), [2, 3])
            check(buffer[0:-1], [2, 3])
            check(buffer.get(slice(0, -2)), [2])
            check(buffer[0:-2], [2])
            check(buffer.get(slice(1, -1)), [3])
            check(buffer[1:-1], [3])

        # Check the type on the finalized buffer (numpy arrays).
        data = buffer.get([1, 0, 2])
        self.assertTrue(isinstance(data, np.ndarray))
        check(data, [3, 2, 4])

    def test_get_with_lookback_and_fill(self):
        """Tests the `fill` argument of `get` with a lookback range >0."""
        buffer = BufferWithInfiniteLookback(
            data=[0, 1, 2, 3, 4, 5],
            lookback=3,
            # Specify a space, so we can fill and one-hot discrete data properly.
            space=gym.spaces.Discrete(6),
        )

        # Test on ongoing and finalized buffer.
        for finalized in [False, True]:
            if finalized:
                buffer.finalize()

            self.assertTrue(len(buffer), 3)

            # Individual indices with fill.
            check(buffer.get(-10, fill=10), 10)
            check(buffer.get(-3, fill=10), 3)
            check(buffer.get(-2, fill=10), 4)
            check(buffer.get(-1, fill=10), 5)
            check(buffer.get(0, fill=10), 3)
            check(buffer.get(2, fill=10), 5)
            check(buffer.get(100, fill=10), 10)

            # Left fill.
            check(buffer.get(slice(-8, None), fill=10), [10, 10, 0, 1, 2, 3, 4, 5])
            check(buffer.get(slice(-9, None), fill=10), [10, 10, 10, 0, 1, 2, 3, 4, 5])
            check(
                buffer.get(slice(-10, None), fill=11),
                [11, 11, 11, 11, 0, 1, 2, 3, 4, 5],
            )
            check(buffer.get(slice(-10, -4), fill=11), [11, 11, 11, 11, 0, 1])
            # Both start stop on left side.
            check(buffer.get(slice(-10, -9), fill=0), [0])
            check(buffer.get(slice(-20, -15), fill=0), [0, 0, 0, 0, 0])
            check(buffer.get(slice(-1001, -1000), fill=6), [6])
            # Both start stop on right side.
            check(buffer.get(slice(10, 15), fill=0), [0, 0, 0, 0, 0])
            check(buffer.get(slice(15, 17), fill=0), [0, 0])
            check(buffer.get(slice(1000, 1001), fill=6), [6])
            # Right fill.
            check(buffer.get(slice(2, 8), fill=12), [5, 12, 12, 12, 12, 12])
            check(buffer.get(slice(1, 7), fill=13), [4, 5, 13, 13, 13, 13])
            check(buffer.get(slice(1, 5), fill=-14), [4, 5, -14, -14])
            # No fill necessary (even though requested).
            check(buffer.get(slice(-5, None), fill=999), [1, 2, 3, 4, 5])
            check(buffer.get(slice(-6, -1), fill=999), [0, 1, 2, 3, 4])
            check(buffer.get(slice(0, 2), fill=999), [3, 4])
            check(buffer.get(slice(1, None), fill=999), [4, 5])
            check(buffer.get(slice(None, 3), fill=999), [3, 4, 5])

        # Check the type on the finalized buffer (numpy arrays).
        data = buffer.get(slice(15, 17), fill=0)
        self.assertTrue(isinstance(data, np.ndarray))
        check(data, [0, 0])

    def test_get_with_fill_and_neg_indices_into_lookback_buffer(self):
        """Tests the `fill` argument of `get` with a lookback range >0."""
        buffer = BufferWithInfiniteLookback(
            data=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            lookback=4,
            # Specify a space, so we can fill and one-hot discrete data properly.
            space=gym.spaces.Discrete(11),
        )

        # Test on ongoing and finalized buffer.
        for finalized in [False, True]:
            if finalized:
                buffer.finalize()

            self.assertTrue(len(buffer), 7)

            # Lokback buffer is [0, 1, 2, 3]
            # Individual indices with negative indices into lookback buffer.
            check(buffer.get(-1, neg_indices_left_of_zero=True), 3)
            check(buffer.get(-2, neg_indices_left_of_zero=True), 2)
            check(buffer.get(-3, neg_indices_left_of_zero=True), 1)
            check(buffer.get(-4, neg_indices_left_of_zero=True), 0)
            check(buffer.get([-1, -3], neg_indices_left_of_zero=True), [3, 1])
            # Slices with negative indices into lookback buffer.
            check(buffer.get(slice(-2, -1), neg_indices_left_of_zero=True), [2])
            check(buffer.get(slice(-2, 0), neg_indices_left_of_zero=True), [2, 3])
            check(
                buffer.get(slice(-2, 4), neg_indices_left_of_zero=True),
                [2, 3, 4, 5, 6, 7],
            )
            check(
                buffer.get(slice(-2, None), neg_indices_left_of_zero=True),
                [2, 3, 4, 5, 6, 7, 8, 9, 10],
            )
            # With left fill.
            check(buffer.get(-8, fill=10, neg_indices_left_of_zero=True), 10)
            check(buffer.get(-800, fill=10, neg_indices_left_of_zero=True), 10)
            check(buffer.get([-8, -1], fill=9, neg_indices_left_of_zero=True), [9, 3])
            check(
                buffer.get(slice(-8, 0), fill=10, neg_indices_left_of_zero=True),
                [10, 10, 10, 10, 0, 1, 2, 3],
            )
            check(
                buffer.get(slice(-7, 1), fill=10, neg_indices_left_of_zero=True),
                [10, 10, 10, 0, 1, 2, 3, 4],
            )
            check(
                buffer.get(slice(-6, None), fill=11, neg_indices_left_of_zero=True),
                [11, 11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            )
            check(
                buffer.get(slice(-10, -4), fill=11, neg_indices_left_of_zero=True),
                [11, 11, 11, 11, 11, 11],
            )
            # Both start stop on left side.
            check(
                buffer.get(slice(-10, -9), fill=0, neg_indices_left_of_zero=True), [0]
            )
            check(
                buffer.get(slice(-20, -15), fill=0, neg_indices_left_of_zero=True),
                [0, 0, 0, 0, 0],
            )
            check(
                buffer.get(slice(-1001, -1000), fill=6, neg_indices_left_of_zero=True),
                [6],
            )
            # Both start stop on right side.
            check(
                buffer.get(slice(10, 15), fill=0, neg_indices_left_of_zero=True),
                [0, 0, 0, 0, 0],
            )
            check(
                buffer.get(slice(15, 17), fill=0, neg_indices_left_of_zero=True), [0, 0]
            )
            check(
                buffer.get(slice(1000, 1001), fill=6, neg_indices_left_of_zero=True),
                [6],
            )
            # Right fill.
            check(buffer.get(8, fill=10, neg_indices_left_of_zero=True), 10)
            check(buffer.get(800, fill=10, neg_indices_left_of_zero=True), 10)
            check(buffer.get([8, 1], fill=9, neg_indices_left_of_zero=True), [9, 5])
            check(
                buffer.get(slice(-2, 8), fill=12, neg_indices_left_of_zero=True),
                [2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
            )
            check(
                buffer.get(slice(-1, 9), fill=13, neg_indices_left_of_zero=True),
                [3, 4, 5, 6, 7, 8, 9, 10, 13, 13],
            )
            check(
                buffer.get(slice(-1, 5), fill=-14, neg_indices_left_of_zero=True),
                [3, 4, 5, 6, 7, 8],
            )
            # No fill necessary (even though requested).
            check(
                buffer.get(slice(-1, None), fill=999, neg_indices_left_of_zero=True),
                [3, 4, 5, 6, 7, 8, 9, 10],
            )
            check(
                buffer.get(slice(-4, -1), fill=999, neg_indices_left_of_zero=True),
                [0, 1, 2],
            )
            check(
                buffer.get(slice(-1, 2), fill=999, neg_indices_left_of_zero=True),
                [3, 4, 5],
            )

        # Check the type on the finalized buffer (numpy arrays).
        data = buffer.get(slice(-17, -15), fill=0, neg_indices_left_of_zero=True)
        self.assertTrue(isinstance(data, np.ndarray))
        check(data, [0, 0])
        data = buffer.get([-3, -1], fill=0, neg_indices_left_of_zero=True)
        self.assertTrue(isinstance(data, np.ndarray))
        check(data, [1, 3])

    def test_get_with_fill_0_and_zero_hot(self):
        """Tests, whether zero-hot is properly done when fill=0."""
        buffer = BufferWithInfiniteLookback(
            data=[0, 1, 0, 1],
            # Specify a space, so we can fill and one-hot discrete data properly.
            space=gym.spaces.Discrete(2),
        )

        # Test on ongoing and finalized buffer.
        for finalized in [False, True]:
            if finalized:
                buffer.finalize()

            self.assertTrue(len(buffer), 4)

            # Right side fill 0. Should be zero-hot.
            check(buffer.get(4, fill=0, one_hot_discrete=True), [0, 0])
            check(
                buffer.get(
                    -1,
                    neg_indices_left_of_zero=True,
                    fill=0,
                    one_hot_discrete=True,
                ),
                [0, 0],
            )

    def test_get_with_complex_space(self):
        """Tests, whether zero-hot is properly done when fill=0."""
        buffer = BufferWithInfiniteLookback(
            data=[
                get_dummy_batch_for_space(
                    space=self.space,
                    batch_size=0,
                    fill_value=float(i),
                )
                for i in range(4)
            ],
            lookback=2,
            # Specify a space, so we can fill and one-hot discrete data properly.
            space=self.space,
        )

        buffer_0 = {
            "a": 0,
            "b": np.array([[0, 0, 0], [0, 0, 0]]),
            "c": (np.array([0, 0]), np.array([0])),
        }
        buffer_0_one_hot = {
            "a": np.array([0.0, 0.0, 0.0, 0.0]),
            "b": np.array([[0, 0, 0], [0, 0, 0]]),
            "c": (np.array([0, 0, 0, 0, 0]), np.array([0])),
        }
        buffer_1 = {
            "a": 1,
            "b": np.array([[1, 1, 1], [1, 1, 1]]),
            "c": (np.array([1, 1]), np.array([1])),
        }
        buffer_2 = {
            "a": 2,
            "b": np.array([[2, 2, 2], [2, 2, 2]]),
            "c": (np.array([2, 2]), np.array([2])),
        }
        buffer_3 = {
            "a": 3,
            "b": np.array([[3, 3, 3], [3, 3, 3]]),
            "c": (np.array([3, 3]), np.array([3])),
        }

        # Test on ongoing and finalized buffer.
        for finalized in [False, True]:
            if finalized:
                buffer.finalize()

                def batch_(s):
                    return batch(s)

            else:

                def batch_(s):
                    return s

            self.assertTrue(len(buffer), 2)

            check(buffer.get(-1), buffer_3)
            check(buffer.get(-2), buffer_2)
            check(buffer.get(-3), buffer_1)
            check(buffer.get(-4), buffer_0)
            check(buffer.get(-5, fill=0.0), buffer_0)
            check(buffer.get([-5, 5], fill=0.0), batch_([buffer_0, buffer_0]))
            check(buffer.get([-5, 1], fill=0.0), batch_([buffer_0, buffer_3]))
            check(buffer.get([1, -10], fill=0.0), batch_([buffer_3, buffer_0]))
            check(
                buffer.get([-10], fill=0.0, one_hot_discrete=True),
                batch_([buffer_0_one_hot]),
            )
            check(buffer.get(slice(0, 1), fill=0.0), batch_([buffer_2]))
            check(buffer.get(slice(1, 3), fill=0.0), batch_([buffer_3, buffer_0]))
            check(buffer.get(slice(-10, -12), fill=0.0), batch_([buffer_0, buffer_0]))
            check(
                buffer.get(slice(-10, -12), fill=0.0, neg_indices_left_of_zero=True),
                batch_([buffer_0, buffer_0]),
            )
            check(
                buffer.get(slice(100, 98), fill=0.0, neg_indices_left_of_zero=True),
                batch_([buffer_0, buffer_0]),
            )
            check(
                buffer.get(slice(100, 98), fill=0.0),
                batch_([buffer_0, buffer_0]),
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
