import pytest
import sys
from unittest.mock import patch, MagicMock

from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray._private.accelerators.utils import (
    resolve_and_update_accelerator_resources,
    get_current_node_accelerator_type,
)


@patch("ray._private.usage.usage_lib.record_hardware_usage")
@patch("ray._private.accelerators.utils.get_current_node_accelerator")
def test_resolve_and_update_accelerators(mock_get_mgr, mock_record_usage):
    mock_mgr = MagicMock()
    mock_mgr.get_resource_name.return_value = "GPU"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0", "1"]
    mock_mgr.get_current_node_num_accelerators.return_value = 2
    mock_mgr.get_current_node_accelerator_type.return_value = "A100"
    mock_mgr.get_current_node_additional_resources.return_value = {"tensor_cores": 4}
    mock_mgr.get_visible_accelerator_ids_env_var.return_value = "CUDA_VISIBLE_DEVICES"

    mock_get_mgr.return_value = (mock_mgr, "GPU", 2)

    resources = {}
    num_gpus = resolve_and_update_accelerator_resources(1, resources)

    assert num_gpus == 1
    assert resources["tensor_cores"] == 4
    assert resources[f"{RESOURCE_CONSTRAINT_PREFIX}A100"] == 1


@patch("ray._private.usage.usage_lib.record_hardware_usage")
@patch("ray._private.accelerators.utils.get_current_node_accelerator")
def test_detect_num_gpus_with_visibility_limit(mock_get_mgr, mock_record_usage):
    mock_mgr = MagicMock()
    mock_mgr.get_resource_name.return_value = "GPU"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0"]
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_node_accelerator_type.return_value = "L4"
    mock_mgr.get_current_node_additional_resources.return_value = {}
    mock_mgr.get_visible_accelerator_ids_env_var.return_value = "CUDA_VISIBLE_DEVICES"

    mock_get_mgr.return_value = (mock_mgr, "GPU", 1)

    resources = {}
    num_gpus = resolve_and_update_accelerator_resources(None, resources)
    assert num_gpus == 1
    assert resources[f"{RESOURCE_CONSTRAINT_PREFIX}L4"] == 1


@patch("ray._private.accelerators.utils.get_current_node_accelerator")
def test_resolve_and_update_accelerators_over_request(mock_get_mgr):
    mock_mgr = MagicMock()
    mock_mgr.get_resource_name.return_value = "GPU"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0", "1"]
    mock_mgr.get_current_node_num_accelerators.return_value = 2
    mock_mgr.get_current_node_accelerator_type.return_value = None
    mock_mgr.get_current_node_additional_resources.return_value = {}
    mock_mgr.get_visible_accelerator_ids_env_var.return_value = "CUDA_VISIBLE_DEVICES"

    mock_get_mgr.return_value = (mock_mgr, "GPU", 3)

    # Expect an error since only 2 GPU devices are visible
    resources = {}
    with pytest.raises(ValueError, match="Attempting to start raylet with 3 GPU"):
        resolve_and_update_accelerator_resources(3, resources)


@patch("ray._private.accelerators.utils.get_current_node_accelerator")
def test_get_current_node_accelerator_type_detected(mock_get_mgr):
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_node_accelerator_type.return_value = "TPU-V6E"
    mock_get_mgr.return_value = (mock_mgr, "TPU", 4)

    resources = {"TPU": 4}
    result = get_current_node_accelerator_type(resources)
    assert result == "TPU-V6E"


@patch("ray._private.accelerators.utils.get_current_node_accelerator")
def test_get_current_node_accelerator_type_none(mock_get_mgr):
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 0
    mock_mgr.get_current_node_accelerator_type.return_value = ""
    mock_get_mgr.return_value = (mock_mgr, "TPU", 0)

    result = get_current_node_accelerator_type({})
    assert result is None


@patch("ray._private.accelerators.utils.get_current_node_accelerator")
def test_get_current_node_accelerator_type_no_manager_detected(mock_get_mgr):
    mock_get_mgr.return_value = None
    result = get_current_node_accelerator_type({})
    assert result is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
