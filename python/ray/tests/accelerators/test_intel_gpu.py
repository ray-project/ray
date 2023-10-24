import os
import sys
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import IntelGPUAcceleratorManager
from ray._private.accelerators import NvidiaGPUAcceleratorManager


@patch(
    "ray._private.accelerators.NvidiaGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible_nvidia_gpus(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    ray.init()
    mock_get_num_accelerators.called
    print(NvidiaGPUAcceleratorManager.get_current_node_num_accelerators())
    print(ray._private.accelerators.get_accelerator_manager_for_resource("GPU"))
    print(ray.get_gpu_ids())
    assert ray.available_resources()["GPU"] == 3


@patch(
    "ray._private.accelerators.IntelGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_auto_detected_more_than_visible_intel_gpus(
    mock_get_num_accelerators, monkeypatch, shutdown_only
):
    monkeypatch.setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:0,1,2")
    ray.init()
    mock_get_num_accelerators.called
    print(IntelGPUAcceleratorManager.get_current_node_num_accelerators())
    print(ray._private.accelerators.get_accelerator_manager_for_resource("GPU"))
    print(ray.get_gpu_ids())
    assert ray.available_resources()["GPU"] == 3


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
