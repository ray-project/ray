from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from typing_extensions import Hashable

import ray
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.util import (
    _op_name_white_list,
    _recorded_operators,
    _recorded_operators_lock,
)
from ray.data._internal.memory_tracing import (
    leak_report,
    trace_allocation,
    trace_deallocation,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.remote_fn import _make_hashable, cached_remote_fn
from ray.data._internal.util import (
    NULL_SENTINEL,
    find_partition_index,
    iterate_with_retry,
    merge_resources_to_ray_remote_args,
    rows_same,
)
from ray.data.tests.conftest import *  # noqa: F401, F403


def _check_usage_record(op_names: List[str], clear_after_check: Optional[bool] = True):
    """Check if operators with given names in `op_names` have been used.
    If `clear_after_check` is True, we clear the list of recorded operators
    (so that subsequent checks do not use existing records of operator usage)."""
    for op_name in op_names:
        assert op_name in _op_name_white_list
        with _recorded_operators_lock:
            assert _recorded_operators.get(op_name, 0) > 0, (
                op_name,
                _recorded_operators,
            )
    if clear_after_check:
        with _recorded_operators_lock:
            _recorded_operators.clear()


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

    assert NULL_SENTINEL != NULL_SENTINEL
    assert NULL_SENTINEL < NULL_SENTINEL
    assert NULL_SENTINEL <= NULL_SENTINEL
    assert not NULL_SENTINEL > NULL_SENTINEL
    assert not NULL_SENTINEL >= NULL_SENTINEL

    # With NoneType
    assert None > NULL_SENTINEL
    assert None >= NULL_SENTINEL
    assert NULL_SENTINEL < None
    assert NULL_SENTINEL <= None
    assert NULL_SENTINEL != None  # noqa: E711

    # With np.nan
    assert np.nan > NULL_SENTINEL
    assert np.nan >= NULL_SENTINEL
    assert NULL_SENTINEL < np.nan
    assert NULL_SENTINEL <= np.nan
    assert NULL_SENTINEL != np.nan

    # Rest
    assert NULL_SENTINEL > 1000
    assert NULL_SENTINEL > "abc"
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
    read_op = Read(
        datasource=datasource,
        datasource_or_legacy_reader=datasource,
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
    assert find_partition_index(table, (None,), sort_key) == 3


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


def test_merge_resources_to_ray_remote_args():
    ray_remote_args = {}
    ray_remote_args = merge_resources_to_ray_remote_args(1, 1, 1, ray_remote_args)
    assert ray_remote_args == {"num_cpus": 1, "num_gpus": 1, "memory": 1}

    ray_remote_args = {"other_resource": 1}
    ray_remote_args = merge_resources_to_ray_remote_args(1, 1, 1, ray_remote_args)
    assert ray_remote_args == {
        "num_cpus": 1,
        "num_gpus": 1,
        "memory": 1,
        "other_resource": 1,
    }


@pytest.mark.parametrize(
    "actual, expected, expected_equal",
    [
        (pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [1]}), True),
        # Different value.
        (pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [2]}), False),
        # Extra column.
        (pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [1], "b": [2]}), False),
        # Different number of rows.
        (pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [1, 1]}), False),
        # Same rows, but different order.
        (pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [2, 1]}), True),
    ],
)
def test_rows_same(actual: pd.DataFrame, expected: pd.DataFrame, expected_equal: bool):
    assert rows_same(actual, expected) == expected_equal


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
