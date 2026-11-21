"""Hardware-free tests for the Ray-side glue of the XCCL backend.

These need neither an XPU nor an XCCL-enabled torch build:
    python -m pytest python/ray/util/collective/tests/test_xccl_unit.py
"""

from unittest.mock import MagicMock, patch

import pytest

import ray.util.collective.collective_group.xccl_collective_group as xccl


def _namespace(name):
    ctx = MagicMock()
    ctx.namespace = name
    return patch.object(xccl.ray, "get_runtime_context", return_value=ctx)


def test_store_key_is_isolated_per_namespace():
    with _namespace("job_a"):
        key_a = xccl._store_key("default")
    with _namespace("job_b"):
        key_b = xccl._store_key("default")
    assert key_a != key_b
    assert "job_a" in key_a and "default" in key_a


def test_store_key_is_unambiguous_when_names_contain_the_separator():
    with _namespace("a:b"):
        key_1 = xccl._store_key("c")
    with _namespace("a"):
        key_2 = xccl._store_key("b:c")
    assert key_1 != key_2


@pytest.mark.parametrize(
    "dist_available,compiled,xpu,expected",
    [
        (True, True, False, True),  # XPU-less driver, XCCL-capable torch
        (True, True, True, True),
        (True, False, True, False),  # XPU present but torch lacks XCCL
        (False, True, True, False),
    ],
)
def test_availability_is_about_xccl_support_not_local_xpu(
    dist_available, compiled, xpu, expected
):
    c10d = MagicMock(spec=["ProcessGroupXCCL"] if compiled else [])
    with patch.object(
        xccl.dist, "is_available", return_value=dist_available
    ), patch.object(xccl, "c10d", c10d), patch.object(
        xccl, "_xpu_available", return_value=xpu
    ):
        assert xccl.XCCLGroup.check_backend_availability() is expected


def test_worker_without_xpu_fails_with_a_clear_error():
    with patch.object(xccl, "_xpu_available", return_value=False):
        with pytest.raises(RuntimeError, match="needs an XPU"):
            xccl.XCCLGroup(2, 0, "g")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
