import pytest
import ray
import numpy as np

from ray.data._internal.util import _check_pyarrow_version
from ray.data._internal.memory_tracing import (
    trace_allocation,
    trace_deallocation,
    leak_report,
)
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
    ctx = ray.data.context.DatasetContext.get_current()
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
