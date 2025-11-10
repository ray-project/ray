import numpy as np

from ray.data.block import _get_group_boundaries_sorted_numpy


def test_groupby_map_groups_get_block_boundaries():
    """Test for cases with Nan or None"""
    indices = _get_group_boundaries_sorted_numpy(
        [
            np.array([1, 1, 2, 2, 3, 3]),
            np.array([1, 1, 2, 2, 3, 4]),
        ]
    )

    assert list(indices) == [0, 2, 4, 5, 6]

    indices = _get_group_boundaries_sorted_numpy(
        [
            np.array([1, 1, 2, 2, 3, 3]),
            np.array(["a", "b", "a", "a", "b", "b"]),
        ]
    )

    assert list(indices) == [0, 1, 2, 4, 6]

    indices = _get_group_boundaries_sorted_numpy([np.array([1, 1, 2, 2, 3, 3])])

    assert list(indices) == [0, 2, 4, 6]


def test_groupby_map_groups_get_block_boundaries_with_nan():
    """Test for cases with Nan or None. Since the arrays are sorted
    in the groupby, they are located at the end. Also, nans/None are
    treated as the same group.
    """

    indices = _get_group_boundaries_sorted_numpy(
        [
            np.array([1, 1, 2, 2, 3, np.nan, np.nan]),
            np.array([1, 1, 2, 2, 3, 4, np.nan]),
        ]
    )

    assert list(indices) == [0, 2, 4, 5, 6, 7]

    indices = _get_group_boundaries_sorted_numpy(
        [
            np.array([1, 1, 2, 2, 3, 3, np.nan]),
            np.array(["a", "b", "a", "a", "b", "b", None]),
        ]
    )

    assert list(indices) == [0, 1, 2, 4, 6, 7]

    indices = _get_group_boundaries_sorted_numpy(
        [
            np.array([1, 1, 2, 2, 3, 3, 4]),
            np.array(["a", "b", "a", "a", "b", "b", None]),
        ]
    )

    assert list(indices) == [0, 1, 2, 4, 6, 7]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
