import unittest

import numpy as np

from ray.rllib.utils.postprocessing.zero_padding import split_and_zero_pad
from ray.rllib.utils.spaces.space_utils import BatchedNdArray, batch


class TestZeroPadding(unittest.TestCase):
    def test_single_batched_array(self):
        cases = [
            (np.arange(7, dtype=np.int16), 3),
            (np.arange(24, dtype=np.float32).reshape(8, 3)[::-2], 2),
            (np.array([[True, False], [False, True], [True, True]]), 8),
        ]

        for values, max_seq_len in cases:
            with self.subTest(shape=values.shape, max_seq_len=max_seq_len):
                item = BatchedNdArray(values)
                actual = split_and_zero_pad([item], max_seq_len)

                expected = []
                for start in range(0, len(values), max_seq_len):
                    chunk = np.zeros(
                        (max_seq_len, *values.shape[1:]), dtype=values.dtype
                    )
                    values_chunk = values[start : start + max_seq_len]
                    chunk[: len(values_chunk)] = values_chunk
                    expected.append(chunk)

                self.assertEqual(len(actual), len(expected))
                for actual_chunk, expected_chunk in zip(actual, expected):
                    np.testing.assert_array_equal(actual_chunk, expected_chunk)
                    self.assertEqual(actual_chunk.dtype, values.dtype)
                    self.assertFalse(np.shares_memory(actual_chunk, item))
                np.testing.assert_array_equal(
                    batch(actual, individual_items_already_have_batch_dim="auto"),
                    np.stack(expected),
                )

    def test_multiple_batched_arrays(self):
        actual = split_and_zero_pad(
            [
                BatchedNdArray([0, 1]),
                BatchedNdArray([2, 3, 4]),
            ],
            max_seq_len=4,
        )

        np.testing.assert_array_equal(actual, [[0, 1, 2, 3], [4, 0, 0, 0]])


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
