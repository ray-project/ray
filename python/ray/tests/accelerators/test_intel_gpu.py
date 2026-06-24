import os
import sys
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators import (
    IntelGPUAcceleratorManager as Accelerator,
    get_accelerator_manager_for_resource,
)
from ray.util.accelerators import INTEL_MAX_1100, INTEL_MAX_1550


@pytest.fixture(autouse=False)
def clean_accelerator_env():
    """Restore all Intel GPU env vars after each test, even if the test fails."""
    keys = (
        "ZE_AFFINITY_MASK",
        "ONEAPI_DEVICE_SELECTOR",
        "RAY_EXPERIMENTAL_NOSET_ZE_AFFINITY_MASK",
        "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR",
    )
    saved = {k: os.environ.get(k) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_visible_intel_gpu_ids(shutdown_only, clean_accelerator_env):
    with patch.object(Accelerator, "get_current_node_num_accelerators", return_value=4):
        os.environ["ONEAPI_DEVICE_SELECTOR"] = "level_zero:0,1,2"
        # Delete the cache so it can be re-populated the next time
        # we call get_accelerator_manager_for_resource
        del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager
        ray.init()
        manager = get_accelerator_manager_for_resource("GPU")
        assert manager.get_current_node_num_accelerators() == 4
        assert manager.__name__ == "IntelGPUAcceleratorManager"
        assert ray.available_resources()["GPU"] == 3


def test_visible_intel_gpu_type(shutdown_only, clean_accelerator_env):
    with patch.object(
        Accelerator, "get_current_node_num_accelerators", return_value=4
    ), patch.object(
        Accelerator, "get_current_node_accelerator_type", return_value=INTEL_MAX_1550
    ):
        os.environ["ONEAPI_DEVICE_SELECTOR"] = "level_zero:0,1,2"
        del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager
        ray.init()
        manager = get_accelerator_manager_for_resource("GPU")
        assert manager.get_current_node_accelerator_type() == INTEL_MAX_1550


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_get_current_node_num_accelerators():
    old_dpctl = None
    if "dpctl" in sys.modules:
        old_dpctl = sys.modules["dpctl"]

    sys.modules["dpctl"] = __import__("mock_dpctl_1")
    assert Accelerator.get_current_node_num_accelerators() == 6

    sys.modules["dpctl"] = __import__("mock_dpctl_2")
    assert Accelerator.get_current_node_num_accelerators() == 4

    if old_dpctl is not None:
        sys.modules["dpctl"] = old_dpctl


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_get_current_node_accelerator_type():
    old_dpctl = None
    if "dpctl" in sys.modules:
        old_dpctl = sys.modules["dpctl"]

    sys.modules["dpctl"] = __import__("mock_dpctl_1")
    assert Accelerator.get_current_node_accelerator_type() == INTEL_MAX_1550

    sys.modules["dpctl"] = __import__("mock_dpctl_2")
    assert Accelerator.get_current_node_accelerator_type() == INTEL_MAX_1100

    if old_dpctl is not None:
        sys.modules["dpctl"] = old_dpctl


def test_intel_gpu_accelerator_manager_api():
    assert Accelerator.get_resource_name() == "GPU"
    # Primary env var is now ZE_AFFINITY_MASK (bare IDs, subprocess-safe).
    assert Accelerator.get_visible_accelerator_ids_env_var() == "ZE_AFFINITY_MASK"
    assert Accelerator.validate_resource_request_quantity(0.1) == (True, None)


def test_get_current_process_visible_accelerator_ids(clean_accelerator_env):
    # ZE_AFFINITY_MASK is the primary — read it first.
    os.environ["ZE_AFFINITY_MASK"] = "0,1,2"
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    # No vars set at all — must return None (not empty list).
    del os.environ["ZE_AFFINITY_MASK"]
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    # Empty string means "no devices allowed".
    os.environ["ZE_AFFINITY_MASK"] = ""
    assert Accelerator.get_current_process_visible_accelerator_ids() == []
    del os.environ["ZE_AFFINITY_MASK"]

    # Backward compat: falls back to ONEAPI_DEVICE_SELECTOR when ZE_AFFINITY_MASK absent.
    os.environ["ONEAPI_DEVICE_SELECTOR"] = "level_zero:0,1,2"
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    del os.environ["ONEAPI_DEVICE_SELECTOR"]
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    os.environ["ONEAPI_DEVICE_SELECTOR"] = ""
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    os.environ["ONEAPI_DEVICE_SELECTOR"] = "NoDevFiles"
    assert Accelerator.get_current_process_visible_accelerator_ids() == []


@pytest.mark.parametrize(
    "physical_ids, expected_ze, expected_oneapi",
    [
        # GPU0 only — physical and re-indexed are the same.
        (["0"],       "0",   "level_zero:0"),
        # GPU0 + GPU1 — physical and re-indexed are the same.
        (["0", "1"],  "0,1", "level_zero:0,1"),
        # GPU1 only — ZE carries physical id 1, but ONEAPI must use re-indexed 0.
        # Without re-indexing: ZE=1 + ONEAPI=level_zero:1 -> 0 devices visible.
        (["1"],       "1",   "level_zero:0"),
        # Non-contiguous physical ids — ONEAPI always sequential from 0.
        (["1", "3"],  "1,3", "level_zero:0,1"),
    ],
)
def test_set_current_process_visible_accelerator_ids(
    clean_accelerator_env, physical_ids, expected_ze, expected_oneapi
):
    Accelerator.set_current_process_visible_accelerator_ids(physical_ids)
    assert os.environ["ZE_AFFINITY_MASK"] == expected_ze
    assert os.environ["ONEAPI_DEVICE_SELECTOR"] == expected_oneapi


def test_set_visible_accelerator_ids_noset_ze(clean_accelerator_env):
    # RAY_EXPERIMENTAL_NOSET_ZE_AFFINITY_MASK suppresses only ZE_AFFINITY_MASK.
    os.environ["RAY_EXPERIMENTAL_NOSET_ZE_AFFINITY_MASK"] = "1"
    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert "ZE_AFFINITY_MASK" not in os.environ
    assert os.environ["ONEAPI_DEVICE_SELECTOR"] == "level_zero:0,1"


def test_set_visible_accelerator_ids_noset_oneapi(clean_accelerator_env):
    # RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR suppresses only ONEAPI_DEVICE_SELECTOR.
    os.environ["RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR"] = "1"
    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert "ONEAPI_DEVICE_SELECTOR" not in os.environ
    assert os.environ["ZE_AFFINITY_MASK"] == "0,1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
