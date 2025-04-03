import numpy as np
import pyarrow as pa
import pytest

from ray.anyscale.data._internal.util.numpy import find_insertion_index
from ray.data.block import BlockColumnAccessor


def test_find_insertion_index_single_column_ascending():
    key_column = [np.array([1, 2, 2, 3, 5])]
    descending = [False]
    assert find_insertion_index(key_column, (0,), descending) == 0  # all entries > 0
    assert find_insertion_index(key_column, (2,), descending) == 1  # first match index
    assert (
        find_insertion_index(key_column, (4,), descending) == 4
    )  # belongs after 3, before 5
    assert find_insertion_index(key_column, (6,), descending) == 5  # all entries < 6


def test_find_insertion_index_single_column_descending():
    key_column = [np.array([5, 3, 2, 2, 1])]
    descending = [True]
    assert find_insertion_index(key_column, (6,), descending) == 0  # belongs before 5
    assert find_insertion_index(key_column, (3,), descending) == 2  # after the last 3
    assert find_insertion_index(key_column, (2,), descending) == 4  # after the last 2
    assert find_insertion_index(key_column, (0,), descending) == 5  # all entries > 0


def test_find_insertion_index_multi_column():
    # Table sorted by col1 asc, then col2 desc.
    key_columns = [np.array([1, 1, 1, 2, 2]), np.array([3, 2, 1, 2, 1])]
    sort_key = [False, True]
    # Insert value (1,3) -> belongs before (1,2)
    assert find_insertion_index(key_columns, (1, 3), sort_key) == 0
    # Insert value (1,2) -> belongs after the first (1,3) and before (1,2)
    # because col1 ties, col2 descending
    assert find_insertion_index(key_columns, (1, 2), sort_key) == 1
    # Insert value (2,2) -> belongs right before (2,2) that starts at index 3
    assert find_insertion_index(key_columns, (2, 2), sort_key) == 3
    # Insert value (0, 4) -> belongs at index 0 (all col1 > 0)
    assert find_insertion_index(key_columns, (0, 4), sort_key) == 0
    # Insert value (2,0) -> belongs after (2,1)
    assert find_insertion_index(key_columns, (2, 0), sort_key) == 5


@pytest.mark.parametrize("null", [None, np.nan])
def test_find_insertion_index_table_with_nulls(null):
    # Nulls/NaNs are sorted greater, so they appear after all real values.
    #
    # NOTE: We're converting from PA to NP to make sure None conversion
    #       (to ``np.nan``) is handled appropriately
    key_columns = [
        BlockColumnAccessor.for_column(pa.array([1, 2, 3, null, null])).to_numpy()
    ]
    sort_key = [False]

    # Insert (2,) -> belongs after 1, before 2 => index 1
    # (But the actual find_insertion_index uses the key_column as-is.)
    assert find_insertion_index(key_columns, (2,), sort_key) == 1
    # Insert (4,) -> belongs before any null => index 3
    assert find_insertion_index(key_columns, (4,), sort_key) == 3
    # Insert (None,) -> always belongs at the end (but before other null values)
    assert find_insertion_index(key_columns, (null,), sort_key) == 3


@pytest.mark.parametrize("null", [None, np.nan])
def test_find_insertion_index_null_boundary(null):
    key_columns = [np.array([1, 2, 3])]
    sort_key = [False]

    # Insert (2,) -> belongs after 1, before 2 => index 1
    # (But the actual find_insertion_index uses the key_column as-is.)
    assert find_insertion_index(key_columns, (2,), sort_key) == 1
    # Insert (4,) -> belongs before any null => index 3
    assert find_insertion_index(key_columns, (4,), sort_key) == 3
    # Insert (None,) -> always belongs at the end
    assert find_insertion_index(key_columns, (null,), sort_key) == 3


def test_find_insertion_index_duplicates():
    key_columns = [np.array([2, 2, 2, 2, 2])]
    sort_key = [False]
    # Insert (2,) in a table of all 2's -> first matching index is 0
    assert find_insertion_index(key_columns, (2,), sort_key) == 0
    # Insert (1,) -> belongs at index 0
    assert find_insertion_index(key_columns, (1,), sort_key) == 0
    # Insert (3,) -> belongs at index 5
    assert find_insertion_index(key_columns, (3,), sort_key) == 5


def test_find_insertion_index_duplicates_descending():
    key_columns = [np.array([2, 2, 2, 2, 2])]
    descending = [True]
    # Insert (2,) in a table of all 2's -> belongs at index 5
    assert find_insertion_index(key_columns, (2,), descending) == 5
    # Insert (1,) -> belongs at index 5
    assert find_insertion_index(key_columns, (1,), descending) == 5
    # Insert (3,) -> belongs at index 0
    assert find_insertion_index(key_columns, (3,), descending) == 0
