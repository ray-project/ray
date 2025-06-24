import pytest
import sys
from unittest.mock import patch, MagicMock

from ray._private.accelerators.accelerator_utils import (
    resolve_and_update_accelerator_resources,
    get_first_detectable_accelerator_type,
)


@patch("ray._private.usage.usage_lib.record_hardware_usage")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
def test_resolve_and_update_accelerators_with_gpu(
    mock_get_mgr, mock_get_names, mock_record_usage
):
    mock_mgr = MagicMock()
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0", "1"]
    mock_mgr.get_current_node_num_accelerators.return_value = 2
    mock_mgr.get_current_node_accelerator_type.return_value = "A100"
    mock_mgr.get_current_node_additional_resources.return_value = {"tensor_cores": 4}
    mock_mgr.get_visible_accelerator_ids_env_var.return_value = "CUDA_VISIBLE_DEVICES"

    mock_get_mgr.return_value = mock_mgr
    mock_get_names.return_value = ["GPU"]

    resources = {}
    num_gpus = resolve_and_update_accelerator_resources(1, resources)

    assert num_gpus == 1
    assert resources["tensor_cores"] == 4


@patch("ray._private.usage.usage_lib.record_hardware_usage")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
def test_auto_detect_num_gpus_with_visibility_limit(
    mock_get_mgr, mock_get_names, mock_record_usage
):
    mock_mgr = MagicMock()
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0"]
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_node_accelerator_type.return_value = "L4"
    mock_mgr.get_current_node_additional_resources.return_value = {}
    mock_mgr.get_visible_accelerator_ids_env_var.return_value = "CUDA_VISIBLE_DEVICES"

    mock_get_mgr.return_value = mock_mgr
    mock_get_names.return_value = ["GPU"]

    resources = {}
    num_gpus = resolve_and_update_accelerator_resources(None, resources)
    assert num_gpus == 1


@patch("ray._private.accelerators.get_all_accelerator_resource_names")
@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
def test_resolve_and_update_accelerators_too_many_requested(
    mock_get_mgr, mock_get_names
):
    mock_mgr = MagicMock()
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0", "1"]
    mock_mgr.get_current_node_num_accelerators.return_value = 2
    mock_mgr.get_current_node_accelerator_type.return_value = None
    mock_mgr.get_current_node_additional_resources.return_value = {}
    mock_mgr.get_visible_accelerator_ids_env_var.return_value = "CUDA_VISIBLE_DEVICES"

    mock_get_mgr.return_value = mock_mgr
    mock_get_names.return_value = ["GPU"]

    resources = {}

    with pytest.raises(ValueError, match="Attempting to start raylet with 3 GPU"):
        resolve_and_update_accelerator_resources(3, resources)


@patch("ray._private.accelerators.get_all_accelerator_resource_names")
@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
def test_get_first_detectable_accelerator_type(mock_get_mgr, mock_get_names):
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 2
    mock_mgr.get_current_node_accelerator_type.return_value = "B200"

    mock_get_mgr.return_value = mock_mgr
    mock_get_names.return_value = ["GPU"]

    accelerator_type = get_first_detectable_accelerator_type({"GPU": 2})
    assert accelerator_type == "B200"


@patch("ray._private.accelerators.get_all_accelerator_resource_names")
@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
def test_detect_non_gpu_accelerator_type(mock_get_mgr, mock_get_names):
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_node_accelerator_type.return_value = "TPU-V6E"

    mock_get_mgr.return_value = mock_mgr
    mock_get_names.return_value = ["TPU"]

    accelerator_type = get_first_detectable_accelerator_type({"TPU": 4})
    assert accelerator_type == "TPU-V6E"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
