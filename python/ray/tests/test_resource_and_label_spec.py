import pytest
import sys
import json
from unittest.mock import patch, MagicMock
from ray._private.resource_and_label_spec import ResourceAndLabelSpec
from ray._common.constants import HEAD_NODE_RESOURCE_NAME, NODE_ID_PREFIX
import ray._private.ray_constants as ray_constants


def test_spec_resolved():
    spec = ResourceAndLabelSpec(
        num_cpus=2,
        num_gpus=1,
        memory=1_000_000_000,
        object_store_memory=500_000_000,
        resources={"TPU": 5},
        labels={},
    )
    assert spec.resolved()
    resource_dict = spec.to_resource_dict()
    assert resource_dict["CPU"] == 2
    assert resource_dict["GPU"] == 1
    assert "TPU" in resource_dict


def test_resolved_false_until_resolve():
    spec = ResourceAndLabelSpec()
    assert not spec.resolved()

    # Patch calls that rely on Ray environment for test
    with patch("ray._private.utils.get_num_cpus", return_value=4), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ), patch(
        "ray._private.utils.estimate_available_memory", return_value=10000000
    ), patch(
        "ray._common.utils.get_system_memory", return_value=20000000
    ), patch(
        "ray._private.utils.get_shared_memory_bytes", return_value=50000
    ), patch.object(
        ResourceAndLabelSpec, "_get_current_node_accelerator", return_value=None
    ), patch(
        "ray._private.usage.usage_lib.record_hardware_usage", lambda *_: None
    ):
        spec.resolve(is_head=True)

    # Values set by default should be filled out
    assert spec.resolved()
    assert any(key.startswith(NODE_ID_PREFIX) for key in spec.resources.keys())
    assert HEAD_NODE_RESOURCE_NAME in spec.resources


def test_to_resource_dict_invalid_types():
    spec = ResourceAndLabelSpec(
        num_cpus=1,
        num_gpus=1,
        memory=1000,
        object_store_memory=1000,
        resources={"INVALID_RESOURCE": -1},
        labels={},
    )
    assert spec.resolved()
    with pytest.raises(ValueError):
        spec.to_resource_dict()


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_auto_detect(mock_all_names, mock_get_mgr):
    mock_all_names.return_value = ["GPU", "TPU"]
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_node_accelerator_type.return_value = "TPU"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = [0, 1, 3, 4]
    mock_get_mgr.return_value = mock_mgr

    spec = ResourceAndLabelSpec(resources={})
    result = spec._get_current_node_accelerator()
    assert result == (mock_mgr, 4)


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_from_resources(mock_all_names, mock_get_mgr):
    mock_all_names.return_value = ["A100"]
    mock_mgr = MagicMock()
    mock_get_mgr.return_value = mock_mgr

    spec = ResourceAndLabelSpec(resources={"A100": 3})
    result = spec._get_current_node_accelerator()
    assert result == (mock_mgr, 3)


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_with_visibility_limit(
    mock_all_names, mock_get_mgr
):
    mock_all_names.return_value = ["A100"]
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 5
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = ["0", "1"]
    mock_get_mgr.return_value = mock_mgr

    spec = ResourceAndLabelSpec(resources={})
    result = spec._get_current_node_accelerator()
    assert result == (mock_mgr, 2)


@patch("ray._private.accelerators.get_accelerator_manager_for_resource")
@patch("ray._private.accelerators.get_all_accelerator_resource_names")
def test_get_current_node_accelerator_none(mock_all_names, mock_get_mgr):
    mock_all_names.return_value = ["B200", "TPU-v6e"]
    mock_mgr = MagicMock()
    mock_mgr.get_current_node_num_accelerators.return_value = 0
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = []
    mock_get_mgr.side_effect = lambda name: mock_mgr

    spec = ResourceAndLabelSpec(resources={})
    result = spec._get_current_node_accelerator()
    assert result is None


def test_load_override_env_labels_merges_and_logs(monkeypatch):
    env_data = {"autoscaler-override-label": "example"}
    monkeypatch.setenv(ray_constants.LABELS_ENVIRONMENT_VARIABLE, json.dumps(env_data))
    spec = ResourceAndLabelSpec()
    result = spec._load_env_labels()
    assert result == env_data


@patch("ray._private.usage.usage_lib.record_hardware_usage", lambda *_: None)
def test_resolve_accelerator_resources_sets_num_gpus():
    # num-gpus not passed in but detected from _resolve_accelerator_resources
    spec = ResourceAndLabelSpec(resources={})
    mock_mgr = MagicMock()
    mock_mgr.get_resource_name.return_value = "GPU"
    mock_mgr.get_current_node_accelerator_type.return_value = "A100"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = None
    spec._resolve_accelerator_resources(mock_mgr, 2)
    assert spec.num_gpus == 2
    assert any("A100" in key for key in spec.resources.keys())


@patch("ray._private.usage.usage_lib.record_hardware_usage", lambda *_: None)
def test_resolve_accelerator_resources_respects_configured_num_gpus():
    # num-gpus value set by user should be respected
    spec = ResourceAndLabelSpec(num_gpus=0, resources={})
    mock_mgr = MagicMock()
    mock_mgr.get_resource_name.return_value = "GPU"
    mock_mgr.get_current_node_accelerator_type.return_value = "H100"
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = None
    spec._resolve_accelerator_resources(mock_mgr, 4)
    assert spec.num_gpus == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
