from typing import Any, Dict, Optional

import numpy as np
import pyarrow as pa
import pytest
from typing_extensions import Hashable

import ray
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.memory_tracing import (
    leak_report,
    trace_allocation,
    trace_deallocation,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.remote_fn import _make_hashable, cached_remote_fn
from ray.data._internal.util import (
    NULL_SENTINEL,
    _check_pyarrow_version,
    find_partition_index,
    find_partitions,
    iterate_with_retry,
)
from ray.data.tests.conftest import *  # noqa: F401, F403


def test_cached_remote_fn():
    def foo():
        pass

    cpu_only_foo = cached_remote_fn(foo, num_cpus=1)
    cached_cpu_only_foo = cached_remote_fn(foo, num_cpus=1)

    assert cpu_only_foo == cached_cpu_only_foo

    gpu_only_foo = cached_remote_fn(foo, num_gpus=1)

    assert cpu_only_foo != gpu_only_foo


def test_null_sentinel():
    """Check that NULL_SENTINEL sorts greater than any other value."""
    assert NULL_SENTINEL > 1000
    assert NULL_SENTINEL > "abc"
    assert NULL_SENTINEL == NULL_SENTINEL
    assert NULL_SENTINEL != 1000
    assert NULL_SENTINEL != "abc"
    assert not NULL_SENTINEL < 1000
    assert not NULL_SENTINEL < "abc"
    assert not NULL_SENTINEL <= 1000
    assert not NULL_SENTINEL <= "abc"
    assert NULL_SENTINEL >= 1000
    assert NULL_SENTINEL >= "abc"


def test_make_hashable():
    valid_args = {
        "int": 0,
        "float": 1.2,
        "str": "foo",
        "dict": {
            0: 0,
            1.2: 1.2,
        },
        "list": list(range(10)),
        "tuple": tuple(range(3)),
        "type": Hashable,
    }

    hashable_args = _make_hashable(valid_args)

    assert hash(hashable_args) == hash(
        (
            ("dict", ((0, 0), (1.2, 1.2))),
            ("float", 1.2),
            ("int", 0),
            ("list", (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
            ("str", "foo"),
            ("tuple", (0, 1, 2)),
            ("type", Hashable),
        )
    )

    # Invalid case # 1: can't mix up key types
    invalid_args = {0: 1, "bar": "baz"}

    with pytest.raises(TypeError) as exc_info:
        _make_hashable(invalid_args)

    assert (
        str(exc_info.value) == "'<' not supported between instances of 'str' and 'int'"
    )


def test_check_pyarrow_version_bounds(unsupported_pyarrow_version):
    # Test that pyarrow versions outside of the defined bounds cause an ImportError to
    # be raised.
    with pytest.raises(ImportError):
        _check_pyarrow_version()


def test_check_pyarrow_version_bounds_disabled(
    unsupported_pyarrow_version,
    disable_pyarrow_version_check,
):
    # Test that pyarrow versions outside of the defined bounds DO NOT cause an
    # ImportError to be raised if the environment variable disabling the check is set.

    # Confirm that ImportError is not raised.
    try:
        _check_pyarrow_version()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


def test_check_pyarrow_version_supported():
    # Test that the pyarrow installed in this testing environment satisfies the pyarrow
    # version bounds.
    try:
        _check_pyarrow_version()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


@pytest.mark.parametrize("enabled", [False, True])
def test_memory_tracing(enabled):
    ctx = ray.data.context.DataContext.get_current()
    ctx.trace_allocations = enabled
    ref1 = ray.put(np.zeros(1024 * 1024))
    ref2 = ray.put(np.zeros(1024 * 1024))
    ref3 = ray.put(np.zeros(1024 * 1024))
    trace_allocation(ref1, "test1")
    trace_allocation(ref2, "test2")
    trace_allocation(ref3, "test5")
    trace_deallocation(ref1, "test3", free=False)
    trace_deallocation(ref2, "test4", free=True)
    ray.get(ref1)
    with pytest.raises(ray.exceptions.ObjectFreedError):
        ray.get(ref2)
    report = leak_report()
    print(report)

    if enabled:
        assert "Leaked object, created at test1" in report, report
        assert "Leaked object, created at test5" in report, report
        assert "Freed object from test2 at test4" in report, report
        assert "skipped dealloc at test3" in report, report
    else:
        assert "test1" not in report, report
        assert "test2" not in report, report
        assert "test3" not in report, report
        assert "test4" not in report, report
        assert "test5" not in report, report


def get_parquet_read_logical_op(
    ray_remote_args: Optional[Dict[str, Any]] = None,
    **read_kwargs,
) -> Read:
    datasource = ParquetDatasource(paths="example://iris.parquet")
    if "parallelism" not in read_kwargs:
        read_kwargs["parallelism"] = 10
    mem_size = None
    if "mem_size" in read_kwargs:
        mem_size = read_kwargs.pop("mem_size")
    read_op = Read(
        datasource=datasource,
        datasource_or_legacy_reader=datasource,
        mem_size=mem_size,
        ray_remote_args=ray_remote_args,
        **read_kwargs,
    )
    return read_op


@ray.remote(num_cpus=0)
class ConcurrencyCounter:
    def __init__(self):
        self.concurrency = 0
        self.max_concurrency = 0

    def inc(self):
        self.concurrency += 1
        if self.concurrency > self.max_concurrency:
            self.max_concurrency = self.concurrency
        return self.concurrency

    def decr(self):
        self.concurrency -= 1
        return self.concurrency

    def get_max_concurrency(self):
        return self.max_concurrency


def test_iterate_with_retry():
    has_raised_error = False

    class MockIterable:
        """Iterate over the numbers 0, 1, 2, and raise an error on the first iteration
        attempt.
        """

        def __init__(self, fail_at_index=3):
            self._index = -1
            self._fail_at_index = fail_at_index

        def __iter__(self):
            return self

        def __next__(self):
            self._index += 1

            if self._index >= 10:
                raise StopIteration

            nonlocal has_raised_error
            if self._index == self._fail_at_index and not has_raised_error:
                has_raised_error = True
                raise RuntimeError("Transient error")

            return self._index

    expected = list(range(10))
    assert list(iterate_with_retry(MockIterable, description="get item")) == expected

    has_raised_error = False
    assert (
        list(iterate_with_retry(MockIterable, description="get item", max_attempts=2))
        == expected
    )


def test_find_partition_index_single_column_ascending():
    table = pa.table({"value": [1, 2, 2, 3, 5]})
    sort_key = SortKey(key=["value"], descending=[False])
    assert find_partition_index(table, (0,), sort_key) == 0  # all entries > 0
    assert find_partition_index(table, (2,), sort_key) == 1  # first match index
    assert find_partition_index(table, (4,), sort_key) == 4  # belongs after 3, before 5
    assert find_partition_index(table, (6,), sort_key) == 5  # all entries < 6


def test_find_partition_index_single_column_descending():
    table = pa.table({"value": [5, 3, 2, 2, 1]})
    sort_key = SortKey(key=["value"], descending=[True])
    assert find_partition_index(table, (6,), sort_key) == 0  # belongs before 5
    assert find_partition_index(table, (3,), sort_key) == 2  # after the last 3
    assert find_partition_index(table, (2,), sort_key) == 4  # after the last 2
    assert find_partition_index(table, (0,), sort_key) == 5  # all entries > 0


def test_find_partition_index_multi_column():
    # Table sorted by col1 asc, then col2 desc.
    table = pa.table({"col1": [1, 1, 1, 2, 2], "col2": [3, 2, 1, 2, 1]})
    sort_key = SortKey(key=["col1", "col2"], descending=[False, True])
    # Insert value (1,3) -> belongs before (1,2)
    assert find_partition_index(table, (1, 3), sort_key) == 0
    # Insert value (1,2) -> belongs after the first (1,3) and before (1,2)
    # because col1 ties, col2 descending
    assert find_partition_index(table, (1, 2), sort_key) == 1
    # Insert value (2,2) -> belongs right before (2,2) that starts at index 3
    assert find_partition_index(table, (2, 2), sort_key) == 3
    # Insert value (0, 4) -> belongs at index 0 (all col1 > 0)
    assert find_partition_index(table, (0, 4), sort_key) == 0
    # Insert value (2,0) -> belongs after (2,1)
    assert find_partition_index(table, (2, 0), sort_key) == 5


def test_find_partition_index_with_nulls():
    # _NullSentinel is sorted greater, so they appear after all real values.
    table = pa.table({"value": [1, 2, 3, None, None]})
    sort_key = SortKey(key=["value"], descending=[False])
    # Insert (2,) -> belongs after 1, before 2 => index 1
    # (But the actual find_partition_index uses the table as-is.)
    assert find_partition_index(table, (2,), sort_key) == 1
    # Insert (4,) -> belongs before any null => index 3
    assert find_partition_index(table, (4,), sort_key) == 3
    # Insert (None,) -> always belongs at the end
    assert find_partition_index(table, (None,), sort_key) == 5


def test_find_partition_index_duplicates():
    table = pa.table({"value": [2, 2, 2, 2, 2]})
    sort_key = SortKey(key=["value"], descending=[False])
    # Insert (2,) in a table of all 2's -> first matching index is 0
    assert find_partition_index(table, (2,), sort_key) == 0
    # Insert (1,) -> belongs at index 0
    assert find_partition_index(table, (1,), sort_key) == 0
    # Insert (3,) -> belongs at index 5
    assert find_partition_index(table, (3,), sort_key) == 5


def test_find_partition_index_duplicates_descending():
    table = pa.table({"value": [2, 2, 2, 2, 2]})
    sort_key = SortKey(key=["value"], descending=[True])
    # Insert (2,) in a table of all 2's -> belongs at index 5
    assert find_partition_index(table, (2,), sort_key) == 5
    # Insert (1,) -> belongs at index 5
    assert find_partition_index(table, (1,), sort_key) == 5
    # Insert (3,) -> belongs at index 0
    assert find_partition_index(table, (3,), sort_key) == 0


def test_find_partitions_single_column_ascending():
    table = pa.table({"value": [1, 2, 2, 3, 5]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(0,), (2,), (4,), (6,)]
    partitions = find_partitions(table, boundaries, sort_key)
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
    partitions = find_partitions(table, boundaries, sort_key)
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
    partitions = find_partitions(table, boundaries, sort_key)
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
    partitions = find_partitions(table, boundaries, sort_key)
    assert len(partitions) == 5
    assert partitions[0].to_pydict() == {"col1": [], "col2": []}
    assert partitions[1].to_pydict() == {"col1": [2, 2], "col2": [1, 2]}
    assert partitions[2].to_pydict() == {"col1": [1, 1], "col2": [1, 2]}
    assert partitions[3].to_pydict() == {"col1": [1, 1, 1], "col2": [3, 4, 5]}
    assert partitions[4].to_pydict() == {"col1": [], "col2": []}


def test_find_partitions_with_nulls():
    table = pa.table({"value": [1, 2, 3, None, None]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(2,), (4,)]
    partitions = find_partitions(table, boundaries, sort_key)
    assert len(partitions) == 3
    assert partitions[0].to_pydict() == {"value": [1]}  # <2
    assert partitions[1].to_pydict() == {"value": [2, 3]}  # [2, 4)
    assert partitions[2].to_pydict() == {"value": [None, None]}  # >=4


def test_find_partitions_duplicates():
    table = pa.table({"value": [2, 2, 2, 2, 2]})
    sort_key = SortKey(key=["value"], descending=[False])
    boundaries = [(1,), (2,), (3,)]
    partitions = find_partitions(table, boundaries, sort_key)
    assert len(partitions) == 4
    assert partitions[0].to_pydict() == {"value": []}  # <1
    assert partitions[1].to_pydict() == {"value": []}  # [1,2)
    assert partitions[2].to_pydict() == {"value": [2, 2, 2, 2, 2]}  # [2,3)
    assert partitions[3].to_pydict() == {"value": []}  # >=3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
