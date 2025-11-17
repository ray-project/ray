import os
import sys
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators import (
    AMDGPUAcceleratorManager,
    get_accelerator_manager_for_resource,
)


@pytest.mark.parametrize(
    "visible_devices_env_var", ("HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES")
)
@patch(
    "ray._private.accelerators.AMDGPUAcceleratorManager.get_current_node_num_accelerators",  # noqa: E501
    return_value=4,
)
def test_visible_amd_gpu_ids(
    mock_get_num_accelerators, visible_devices_env_var, monkeypatch, shutdown_only
):
    monkeypatch.setenv(visible_devices_env_var, "0,1,2")
    # Delete the cache so it can be re-populated the next time
    # we call get_accelerator_manager_for_resource
    del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager
    ray.init()
    _ = mock_get_num_accelerators.called
    assert ray.available_resources()["GPU"] == 3


@patch(
    "ray._private.accelerators.AMDGPUAcceleratorManager._get_amd_device_ids",
    return_value=["0x74a1", "0x74a1", "0x74a1", "0x74a1"],
)
def test_visible_amd_gpu_type(mock_get_amd_device_ids, shutdown_only):
    ray.init()
    _ = mock_get_amd_device_ids.called
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
    _ = mock_get_num_accelerators.called
    assert AMDGPUAcceleratorManager.get_current_node_accelerator_type() is None


@pytest.mark.parametrize(
    "visible_devices_env_var", ("HIP_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES")
)
def test_get_current_process_visible_accelerator_ids(
    visible_devices_env_var, monkeypatch
):
    monkeypatch.setenv(visible_devices_env_var, "0,1,2")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]

    monkeypatch.setenv(visible_devices_env_var, "0,2,7")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "2",
        "7",
    ]

    monkeypatch.setenv(visible_devices_env_var, "")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []

    del os.environ[visible_devices_env_var]
    assert (
        AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None
    )


def test_hip_cuda_env_var_get_current_process_visible_accelerator_ids(monkeypatch):
    # HIP and CUDA visible env vars are set and equal
    monkeypatch.setenv("HIP_VISIBLE_DEVICES", "0,1,2")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]

    # HIP and CUDA visible env vars are set and not equal
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,3")
    with pytest.raises(ValueError):
        AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids()


def test_set_current_process_visible_accelerator_ids():
    AMDGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0"])
    env_var = AMDGPUAcceleratorManager.get_visible_accelerator_ids_env_var()
    assert os.environ[env_var] == "0"

    AMDGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ[env_var] == "0,1"

    AMDGPUAcceleratorManager.set_current_process_visible_accelerator_ids(
        ["0", "1", "7"]
    )
    assert os.environ[env_var] == "0,1,7"

    del os.environ[env_var]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
