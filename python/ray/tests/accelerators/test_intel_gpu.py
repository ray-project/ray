import os
import sys
import pytest
from unittest.mock import Mock

import ray
from ray._private.accelerators import IntelGPUAcceleratorManager
from ray.util.accelerators import INTEL_MAX_1550


def test_visible_intel_gpu_ids(monkeypatch, shutdown_only):
    IntelGPUAcceleratorManager.get_current_node_num_accelerators = Mock(return_value=4)
    monkeypatch.setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:0,1,2")
    ray.init()
    manager = ray._private.accelerators.get_accelerator_manager_for_resource("GPU")
    assert manager.get_current_node_num_accelerators() == 4
    assert manager.__name__ == "IntelGPUAcceleratorManager"
    assert ray.available_resources()["GPU"] == 3


def test_visible_intel_gpu_type(monkeypatch, shutdown_only):
    IntelGPUAcceleratorManager.get_current_node_num_accelerators = Mock(return_value=4)
    IntelGPUAcceleratorManager.get_current_node_accelerator_type = Mock(
        return_value=INTEL_MAX_1550
    )
    monkeypatch.setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:0,1,2")
    ray.init()
    manager = ray._private.accelerators.get_accelerator_manager_for_resource("GPU")
    assert manager.get_current_node_accelerator_type() == INTEL_MAX_1550


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
