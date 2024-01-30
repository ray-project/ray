from typing import Any, Dict, Optional

import numpy as np
import pytest

import ray
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.memory_tracing import (
    leak_report,
    trace_allocation,
    trace_deallocation,
)
from ray.data._internal.util import _check_pyarrow_version, _split_list
from ray.data.datasource.parquet_datasource import ParquetDatasource
from ray.data.tests.conftest import *  # noqa: F401, F403


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


def test_list_splits():
    with pytest.raises(AssertionError):
        _split_list(list(range(5)), 0)

    with pytest.raises(AssertionError):
        _split_list(list(range(5)), -1)

    assert _split_list(list(range(5)), 7) == [[0], [1], [2], [3], [4], [], []]
    assert _split_list(list(range(5)), 2) == [[0, 1, 2], [3, 4]]
    assert _split_list(list(range(6)), 2) == [[0, 1, 2], [3, 4, 5]]
    assert _split_list(list(range(5)), 1) == [[0, 1, 2, 3, 4]]
    assert _split_list(["foo", 1, [0], None], 2) == [["foo", 1], [[0], None]]
    assert _split_list(["foo", 1, [0], None], 3) == [["foo", 1], [[0]], [None]]


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
