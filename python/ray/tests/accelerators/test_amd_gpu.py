import os
import sys
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators import AMDGPUAcceleratorManager
from ray._private.test_utils import mock_accelerator_detection


def test_visible_amd_gpu_ids(monkeypatch, shutdown_only):
    monkeypatch.setenv("HIP_VISIBLE_DEVICES", "0,1,2")
    with mock_accelerator_detection(AMDGPUAcceleratorManager, num_accelerators=4):
        ray.init()
        assert ray.available_resources()["GPU"] == 3


def test_visible_amd_gpu_type():
    with patch.object(
        AMDGPUAcceleratorManager,
        "_get_amd_device_ids",
        return_value=["0x74a1", "0x74a1", "0x74a1", "0x74a1"],
    ):
        assert (
            AMDGPUAcceleratorManager.get_current_node_accelerator_type()
            == "AMD-Instinct-MI300X-OAM"
        )


def test_visible_amd_gpu_type_bad_device_id():
    with patch.object(
        AMDGPUAcceleratorManager,
        "_get_amd_device_ids",
        return_value=["0x640f", "0x640f", "0x640f", "0x640f"],
    ):
        assert AMDGPUAcceleratorManager.get_current_node_accelerator_type() is None


def test_get_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.setenv("HIP_VISIBLE_DEVICES", "0,1,2")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "1",
        "2",
    ]

    monkeypatch.setenv("HIP_VISIBLE_DEVICES", "0,2,7")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "2",
        "7",
    ]

    monkeypatch.setenv("HIP_VISIBLE_DEVICES", "")
    assert AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []

    del os.environ["HIP_VISIBLE_DEVICES"]
    assert (
        AMDGPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None
    )


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
