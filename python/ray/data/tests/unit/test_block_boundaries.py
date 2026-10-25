import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.arrow_block import ArrowBlockAccessor
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


@pytest.mark.parametrize(
    "column,expected",
    [
        # fixed-width numeric
        (pa.array([1, 1, 2, 2, 3]), [0, 2, 4, 5]),
        (pa.array([1, 2, 3]), [0, 1, 2, 3]),
        (pa.array([7, 7, 7]), [0, 3]),
        # nulls sort last and form one group
        (pa.array([1, 1, None, None]), [0, 2, 4]),
        # NaN and null are distinct groups
        (pa.array([1.0, float("nan"), float("nan"), None]), [0, 1, 3, 4]),
        # variable-width
        (pa.array(["a", "a", "b", "c", "c"]), [0, 2, 3, 5]),
        (pa.array([b"a", b"a", b"b"]), [0, 2, 3]),
        (pa.array([False, True, True]), [0, 1, 3]),
    ],
)
def test_arrow_group_boundaries(column, expected):
    table = pa.table({"k": column})
    acc = ArrowBlockAccessor(table)
    assert list(acc._get_group_boundaries_sorted(["k"])) == expected


def test_arrow_group_boundaries_multiple_keys():
    table = pa.table({"k1": [1, 1, 1, 2, 2], "k2": ["a", "a", "b", "a", "a"]})
    acc = ArrowBlockAccessor(table)
    assert list(acc._get_group_boundaries_sorted(["k1", "k2"])) == [0, 2, 3, 5]


def test_arrow_group_boundaries_edge_cases():
    acc = ArrowBlockAccessor(pa.table({"k": pa.array([], type=pa.int64())}))
    assert list(acc._get_group_boundaries_sorted(["k"])) == []

    # No keys means the whole block is a single group.
    acc = ArrowBlockAccessor(pa.table({"k": [1, 2, 3]}))
    assert list(acc._get_group_boundaries_sorted([])) == [0, 3]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
