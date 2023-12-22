import os
import sys
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import AMDGPUAcceleratorManager as Accelerator


def test_visible_amd_gpu_ids(shutdown_only):
    with patch.object(Accelerator, "get_current_node_num_accelerators", return_value=4):
        os.environ["ROCR_VISIBLE_DEVICES"] = "0,1,2"
        ray.init()
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("GPU")
        assert manager.get_current_node_num_accelerators() == 4
        assert manager.__name__ == "AMDGPUAcceleratorManager"
        assert ray.available_resources()["GPU"] == 3


def test_visible_amd_gpu_type(shutdown_only):
    with patch.object(
        Accelerator,
        "_get_amd_pci_ids",
        return_value={
            "card0": {"GPU ID": "0x740f"},
            "card1": {"GPU ID": "0x740f"},
            "card2": {"GPU ID": "0x740f"},
            "card3": {"GPU ID": "0x740f"},
        },
    ):
        ray.init()
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("GPU")
        assert manager.get_current_node_accelerator_type() == "AMD Instinct MI210"

    with patch.object(
        Accelerator, "_get_amd_pci_ids", return_value={"card0": {"GPU ID": "0x640f"}}
    ):
        manager = None
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("GPU")
        assert manager.get_current_node_accelerator_type() is None

    with patch.object(Accelerator, "_get_amd_pci_ids", return_value=None):
        manager = None
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("GPU")
        assert manager.get_current_node_accelerator_type() is None


def test_amd_gpu_accelerator_manager_api():
    assert Accelerator.get_resource_name() == "GPU"
    assert Accelerator.get_visible_accelerator_ids_env_var() == "ROCR_VISIBLE_DEVICES"
    assert Accelerator.validate_resource_request_quantity(0.1) == (True, None)


def test_get_current_process_visible_accelerator_ids():
    os.environ["ROCR_VISIBLE_DEVICES"] = "0,1,2"
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    os.environ["ROCR_VISIBLE_DEVICES"] = "0,2,7"
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "2", "7"]

    del os.environ["ROCR_VISIBLE_DEVICES"]
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    os.environ["ROCR_VISIBLE_DEVICES"] = ""
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    del os.environ["ROCR_VISIBLE_DEVICES"]


def test_set_current_process_visible_accelerator_ids():
    Accelerator.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["ROCR_VISIBLE_DEVICES"] == "0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["ROCR_VISIBLE_DEVICES"] == "0,1"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1", "7"])
    assert os.environ["ROCR_VISIBLE_DEVICES"] == "0,1,7"

    del os.environ["ROCR_VISIBLE_DEVICES"]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
