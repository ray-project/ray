import os
import sys
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import IntelGPUAcceleratorManager as Accelerator
from ray._private.accelerators import get_accelerator_manager_for_resource
from ray.util.accelerators import INTEL_MAX_1550, INTEL_MAX_1100


def test_visible_intel_gpu_ids(shutdown_only):
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


def test_visible_intel_gpu_type(shutdown_only):
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
    assert Accelerator.get_visible_accelerator_ids_env_var() == "ONEAPI_DEVICE_SELECTOR"
    assert Accelerator.validate_resource_request_quantity(0.1) == (True, None)


def test_get_current_process_visible_accelerator_ids():
    os.environ["ONEAPI_DEVICE_SELECTOR"] = "level_zero:0,1,2"
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    del os.environ["ONEAPI_DEVICE_SELECTOR"]
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    os.environ["ONEAPI_DEVICE_SELECTOR"] = ""
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    os.environ["ONEAPI_DEVICE_SELECTOR"] = "NoDevFiles"
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    del os.environ["ONEAPI_DEVICE_SELECTOR"]


def test_set_current_process_visible_accelerator_ids():
    Accelerator.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["ONEAPI_DEVICE_SELECTOR"] == "level_zero:0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["ONEAPI_DEVICE_SELECTOR"] == "level_zero:0,1"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1", "2"])
    assert os.environ["ONEAPI_DEVICE_SELECTOR"] == "level_zero:0,1,2"

    del os.environ["ONEAPI_DEVICE_SELECTOR"]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
