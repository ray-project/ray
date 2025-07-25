import sys
import json
import pytest
from unittest.mock import patch
from ray._common.constants import HEAD_NODE_RESOURCE_NAME, NODE_ID_PREFIX
import ray._private.ray_constants as ray_constants
from ray._private.resource_and_label_spec import ResourceAndLabelSpec


def test_resource_and_label_spec_resolves(monkeypatch):
    """Validate that ResourceAndLabelSpec resolve() fills out defaults."""
    monkeypatch.setenv(ray_constants.LABELS_ENVIRONMENT_VARIABLE, "{}")

    with patch("ray._private.utils.get_num_cpus", return_value=4), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ), patch(
        "ray._private.utils.estimate_available_memory", return_value=1_000_000_000
    ), patch(
        "ray._common.utils.get_system_memory", return_value=2_000_000_000
    ), patch(
        "ray._private.utils.get_shared_memory_bytes", return_value=50_000
    ):
        spec = ResourceAndLabelSpec()
        spec.resolve(is_head=True)

    assert spec.resolved()
    assert spec.num_cpus == 4
    assert spec.num_gpus == 0
    assert isinstance(spec.labels, dict)
    assert HEAD_NODE_RESOURCE_NAME in spec.resources
    assert any(key.startswith(NODE_ID_PREFIX) for key in spec.resources.keys())


def test_to_resource_dict_with_invalid_types():
    """Validate malformed resource values raise ValueError from to_resource_dict()."""
    spec = ResourceAndLabelSpec(
        num_cpus=1,
        num_gpus=1,
        memory=1_000,
        object_store_memory=1_000,
        resources={"INVALID": -5},  # Invalid
        labels={},
    )
    spec.resolve(is_head=True, node_ip_address="127.0.0.1")
    with pytest.raises(ValueError):
        spec.to_resource_dict()


def test_label_spec_resolve_merged_env_labels(monkeypatch):
    """Validate that LABELS_ENVIRONMENT_VARIABLE is merged into final labels."""
    override_labels = {"autoscaler-override-label": "example"}
    monkeypatch.setenv(
        ray_constants.LABELS_ENVIRONMENT_VARIABLE, json.dumps(override_labels)
    )

    with patch("ray._private.utils.get_num_cpus", return_value=1), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ), patch(
        "ray._private.utils.estimate_available_memory", return_value=1_000_000
    ), patch(
        "ray._common.utils.get_system_memory", return_value=2_000_000
    ), patch(
        "ray._private.utils.get_shared_memory_bytes", return_value=50_000
    ):
        spec = ResourceAndLabelSpec()
        spec.resolve(is_head=True)

    assert any(key == "autoscaler-override-label" for key in spec.labels)


@patch("ray._private.usage.usage_lib.record_hardware_usage", lambda *_: None)
def test_resolve_sets_accelerator_resources(monkeypatch):
    """Verify that GPUs/TPU values are auto-detected and assigned properly."""
    monkeypatch.setenv(ray_constants.LABELS_ENVIRONMENT_VARIABLE, "{}")

    # Mock a node with GPUs with 4 visible IDs
    mock_mgr = (
        patch("ray._private.accelerators.get_accelerator_manager_for_resource")
        .start()
        .return_value
    )
    patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["GPU"],
    ).start()
    mock_mgr.get_current_node_num_accelerators.return_value = 4
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = [
        "0",
        "1",
        "2",
        "3",
    ]
    mock_mgr.get_resource_name.return_value = "GPU"
    mock_mgr.get_current_node_accelerator_type.return_value = "A100"

    with patch("ray._private.utils.get_num_cpus", return_value=2), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ), patch("ray._private.utils.estimate_available_memory", return_value=10000), patch(
        "ray._common.utils.get_system_memory", return_value=20000
    ), patch(
        "ray._private.utils.get_shared_memory_bytes", return_value=50000
    ):
        spec = ResourceAndLabelSpec()
        spec.resolve(is_head=False)

    assert spec.num_gpus == 4
    assert any("A100" in key for key in spec.resources)


def test_resolve_raises_if_exceeds_visible_devices(monkeypatch):
    """Check that ValueError is raised when requested accelerators exceed visible IDs."""
    monkeypatch.setenv(ray_constants.LABELS_ENVIRONMENT_VARIABLE, "{}")

    mock_mgr = (
        patch("ray._private.accelerators.get_accelerator_manager_for_resource")
        .start()
        .return_value
    )
    patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["GPU"],
    ).start()
    mock_mgr.get_current_node_num_accelerators.return_value = 5
    mock_mgr.get_current_process_visible_accelerator_ids.return_value = [
        "0",
        "1",
    ]  # 2 visible IDs
    mock_mgr.get_resource_name.return_value = "GPU"

    spec = ResourceAndLabelSpec()
    spec.num_gpus = 3  # request 3 GPUs

    with patch("ray._private.utils.get_num_cpus", return_value=2), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ), patch(
        "ray._private.utils.estimate_available_memory", return_value=1_000_000
    ), patch(
        "ray._common.utils.get_system_memory", return_value=2_000_000
    ), patch(
        "ray._private.utils.get_shared_memory_bytes", return_value=50_000
    ):
        with pytest.raises(ValueError, match="Attempting to start raylet"):
            spec.resolve(is_head=False)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
