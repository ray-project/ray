import unittest

from ray.rllib.utils.filter import NoFilter


class _BadArray:
    def __array__(self, dtype=None, copy=None):
        raise TypeError("cannot convert")


class TestNoFilter(unittest.TestCase):
    def test_failed_array_conversion_error_message(self):
        value = _BadArray()

        with self.assertRaisesRegex(ValueError, "Failed to convert to array:"):
            NoFilter()(value)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
