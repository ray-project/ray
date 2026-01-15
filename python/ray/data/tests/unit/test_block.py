from datetime import datetime

import numpy as np
import pyarrow as pa
import pytest

from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import BlockAccessor, BlockColumnAccessor


def test_find_partitions_single_column_ascending():
    table = pa.table({"value": [1, 2, 2, 3, 5]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(0,), (2,), (4,), (6,)]
    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )
    assert len(partitions) == 5
    assert partitions[0].to_pydict() == {"value": []}  # <0
    assert partitions[1].to_pydict() == {"value": [1]}  # [0,2)
    assert partitions[2].to_pydict() == {"value": [2, 2, 3]}  # [2,4)
    assert partitions[3].to_pydict() == {"value": [5]}  # [4,6)
    assert partitions[4].to_pydict() == {"value": []}  # >=6


def test_find_partitions_single_column_descending():
    table = pa.table({"value": [5, 3, 2, 2, 1]})
    sort_key = SortKey(key=["value"], descending=[True])
    boundaries = [(6,), (3,), (2,), (0,)]
    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )
    assert len(partitions) == 5
    assert partitions[0].to_pydict() == {"value": []}  # >=6
    assert partitions[1].to_pydict() == {"value": [5, 3]}  # [3, 6)
    assert partitions[2].to_pydict() == {"value": [2, 2]}  # [2, 3)
    assert partitions[3].to_pydict() == {"value": [1]}  # [0, 2)
    assert partitions[4].to_pydict() == {"value": []}  # <0


def test_find_partitions_multi_column_ascending_first():
    table = pa.table({"col1": [1, 1, 1, 1, 1, 2, 2], "col2": [4, 3, 2.5, 2, 1, 2, 1]})
    sort_key = SortKey(key=["col1", "col2"], descending=[False, True])
    boundaries = [(1, 3), (1, 2), (2, 2), (2, 0)]
    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )
    assert len(partitions) == 5
    assert partitions[0].to_pydict() == {"col1": [1], "col2": [4]}
    assert partitions[1].to_pydict() == {"col1": [1, 1], "col2": [3, 2.5]}
    assert partitions[2].to_pydict() == {"col1": [1, 1], "col2": [2, 1]}
    assert partitions[3].to_pydict() == {"col1": [2, 2], "col2": [2, 1]}
    assert partitions[4].to_pydict() == {"col1": [], "col2": []}


def test_find_partitions_multi_column_descending_first():
    table = pa.table({"col1": [2, 2, 1, 1, 1, 1, 1], "col2": [1, 2, 1, 2, 3, 4, 5]})
    sort_key = SortKey(key=["col1", "col2"], descending=[True, False])
    boundaries = [(2, 0), (2, 2), (1, 2), (1, 6)]
    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )
    assert len(partitions) == 5
    assert partitions[0].to_pydict() == {"col1": [], "col2": []}
    assert partitions[1].to_pydict() == {"col1": [2, 2], "col2": [1, 2]}
    assert partitions[2].to_pydict() == {"col1": [1, 1], "col2": [1, 2]}
    assert partitions[3].to_pydict() == {"col1": [1, 1, 1], "col2": [3, 4, 5]}
    assert partitions[4].to_pydict() == {"col1": [], "col2": []}


@pytest.mark.parametrize("null", [None, np.nan])
def test_find_partitions_table_with_nulls(null):
    table = pa.table({"value": [1, 2, 3, null, null]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(2,), (4,)]

    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )

    assert len(partitions) == 3
    assert partitions[0].to_pydict() == {"value": [1]}  # <2
    assert partitions[1].to_pydict() == {"value": [2, 3]}  # [2, 4)

    if null is None:
        assert partitions[2].to_pydict() == {"value": [null, null]}  # >=4
    else:
        # NOTE: NaNs couldn't be compared directly
        result = partitions[2].to_pydict()
        assert len(result["value"]) == 2 and all([np.isnan(v) for v in result["value"]])


@pytest.mark.parametrize("dtype", ["str", "datetime"])
def test_find_partitions_object_table_with_nulls(dtype):
    if dtype == "str":
        col = pa.array(["a", "b", "c", None, None])
    elif dtype == "datetime":
        col = pa.array(
            [
                datetime.fromordinal(1),
                datetime.fromordinal(2),
                datetime.fromordinal(3),
                None,
                None,
            ]
        )
    else:
        raise ValueError(f"Unexpected dtype={dtype}")

    table = pa.table({"value": col})

    sort_key = SortKey(key=["value"], descending=[False])

    ndarray = BlockColumnAccessor.for_column(col).to_numpy(zero_copy_only=False)

    # Compose boundaries
    boundaries = [(ndarray[1],), (None,)]

    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )

    assert len(partitions) == 3
    assert (
        partitions[0]["value"].to_pylist() == col[0:1].to_pylist()
    )  # < second element
    assert partitions[1]["value"].to_pylist() == col[1:3].to_pylist()  # < None
    assert partitions[2]["value"].to_pylist() == col[3:].to_pylist()  # remaining


@pytest.mark.parametrize("null", [None, np.nan])
def test_find_partitions_null_boundary(null):
    table = pa.table({"value": [1, 2, 3]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(2,), (null,)]

    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )

    assert len(partitions) == 3
    assert partitions[0].to_pydict() == {"value": [1]}  # <2
    assert partitions[1].to_pydict() == {"value": [2, 3]}  # < null (nulls go last)
    assert partitions[2].to_pydict() == {"value": []}


def test_find_partitions_duplicates():
    table = pa.table({"value": [2, 2, 2, 2, 2]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(1,), (2,), (3,)]
    partitions = BlockAccessor.for_block(table)._find_partitions_sorted(
        boundaries, sort_key
    )
    assert len(partitions) == 4
    assert partitions[0].to_pydict() == {"value": []}  # <1
    assert partitions[1].to_pydict() == {"value": []}  # [1,2)
    assert partitions[2].to_pydict() == {"value": [2, 2, 2, 2, 2]}  # [2,3)
    assert partitions[3].to_pydict() == {"value": []}  # >=3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
