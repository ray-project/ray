import os
import sys
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import AMDGPUAcceleratorManager
from ray._private.accelerators import get_accelerator_manager_for_resource


@patch(
    "ray._private.accelerators.AMDGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_visible_amd_gpu_ids(mock_get_num_accelerators, monkeypatch, shutdown_only):
    monkeypatch.setenv("ROCR_VISIBLE_DEVICES", "0,1,2")
    # Delete the cache so it can be re-populated the next time
    # we call get_accelerator_manager_for_resource
    del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager
    ray.init()
    mock_get_num_accelerators.called
    assert ray.available_resources()["GPU"] == 3


@patch(
    "ray._private.accelerators.AMDGPUAcceleratorManager._get_amd_device_ids",
    return_value=["0x74a1", "0x74a1", "0x74a1", "0x74a1"],
)
def test_visible_amd_gpu_type(mock_get_amd_device_ids, shutdown_only):
    ray.init()
    mock_get_amd_device_ids.called
    assert (
        AMDGPUAcceleratorManager.get_current_node_accelerator_type()
        == "AMD-Instinct-MI300X-OAM"
    )


@patch(
    "ray._private.accelerators.AMDGPUAcceleratorManager._get_amd_device_ids",
    return_value=["0x640f", "0x640f", "0x640f", "0x640f"],
)
def test_visible_amd_gpu_type_bad_device_id(mock_get_num_accelerators, shutdown_only):
    ray.init()
    mock_get_num_accelerators.called
    assert AMDGPUAcceleratorManager.get_current_node_accelerator_type() is None


def test_get_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.setenv("ROCR_VISIBLE_DEVICES", "0,1,2")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]

    monkeypatch.setenv("ROCR_VISIBLE_DEVICES", "0,2,7")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "2",
        "7",
    ]

    monkeypatch.setenv("ROCR_VISIBLE_DEVICES", "")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []

    del os.environ["ROCR_VISIBLE_DEVICES"]
    assert (
        AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None
    )


def test_set_current_process_visible_accelerator_ids():
    AMDGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["ROCR_VISIBLE_DEVICES"] == "0"

    AMDGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["ROCR_VISIBLE_DEVICES"] == "0,1"

    AMDGPUAcceleratorManager.set_current_process_visible_accelerator_ids(
        ["0", "1", "7"]
    )
    assert os.environ["ROCR_VISIBLE_DEVICES"] == "0,1,7"

    del os.environ["ROCR_VISIBLE_DEVICES"]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
