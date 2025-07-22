import pytest
import sys
from unittest.mock import patch, MagicMock

from ray._private.accelerators.utils import get_current_node_accelerator


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_auto_detect(mock_all_names, mock_get_mgr):
    # Validate GPU num_accelerators in resource dict is detected and returned
    mock_all_names.return_value = ["GPU", "TPU"]
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_node_accelerator_type.return_value = "TPU"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = [0, 1, 3, 4]
    mock_get_mgr.return_value = mock_mgr

    result = get_current_node_accelerator(None, {})
    assert result == (mock_mgr, 4)


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_from_resources(mock_all_names, mock_get_mgr):
    # Validate GPU num_accelerators in resource dict is detected and returned
    mock_all_names.return_value = ["GPU"]
    mock_mgr = MagicMock()
    mock_get_mgr.return_value = mock_mgr

    resources = {"GPU": 3}
    result = get_current_node_accelerator(None, resources)
    assert result == (mock_mgr, 3)


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_with_visibility_limit(
    mock_all_names, mock_get_mgr
):
    # Check get_current_node_accelerator caps num_accelerators to visible ids
    mock_all_names.return_value = ["GPU"]
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 5
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0", "1"]
    mock_get_mgr.return_value = mock_mgr

    resources = {}
    result = get_current_node_accelerator(None, resources)
    assert result == (mock_mgr, 2)


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_none(mock_all_names, mock_get_mgr):
    # Check get_current_node_accelerator returns None for num_accelerators == 0
    mock_all_names.return_value = ["GPU", "TPU"]
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 0
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = []
    mock_get_mgr.side_effect = lambda name: mock_mgr

    resources = {}
    result = get_current_node_accelerator(None, resources)
    assert result[0] is None and result[1] == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
