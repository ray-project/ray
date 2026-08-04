import os
import sys
from unittest.mock import patch

import pytest

import ray
from ray._private.accelerators import (
    MetaxGPUAcceleratorManager,
    get_accelerator_manager_for_resource,
)


@patch(
    "ray._private.accelerators.MetaxGPUAcceleratorManager.get_current_node_num_accelerators",
    return_value=4,
)
def test_visible_metax_gpu_ids(mock_get_num_accelerators, monkeypatch, shutdown_only):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager
    ray.init()
    assert mock_get_num_accelerators.called
    assert ray.available_resources()["GPU"] == 3


def test_get_current_process_visible_accelerator_ids(monkeypatch):
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    assert MetaxGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0"
    ]

    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,4,7")
    assert MetaxGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
        "0",
        "4",
        "7",
    ]

    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert (
        MetaxGPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []
    )

    monkeypatch.delenv("CUDA_VISIBLE_DEVICES")
    assert (
        MetaxGPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None
    )


def test_set_current_process_visible_accelerator_ids():
    MetaxGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["CUDA_VISIBLE_DEVICES"] == "0"

    MetaxGPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["CUDA_VISIBLE_DEVICES"] == "0,1"

    MetaxGPUAcceleratorManager.set_current_process_visible_accelerator_ids(
        ["0", "1", "7"]
    )
    assert os.environ["CUDA_VISIBLE_DEVICES"] == "0,1,7"

    del os.environ["CUDA_VISIBLE_DEVICES"]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
