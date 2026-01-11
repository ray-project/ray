import json
import sys
from unittest.mock import patch

import pytest

import ray._private.ray_constants as ray_constants
from ray._common.constants import HEAD_NODE_RESOURCE_NAME, NODE_ID_PREFIX
from ray._private.accelerators import AcceleratorManager
from ray._private.resource_and_label_spec import ResourceAndLabelSpec


class FakeAcceleratorManager(AcceleratorManager):
    """Minimal fake Acceleratormanager for testing."""

    # Configure these values to test different resource resolution paths.
    def __init__(
        self,
        resource_name,
        accelerator_type,
        num_accelerators,
        additional_resources=None,
        visible_ids=None,
    ):
        self._resource_name = resource_name
        self._accelerator_type = accelerator_type
        self._num_accelerators = num_accelerators
        self._additional_resources = additional_resources
        self._visible_ids = visible_ids

    def get_current_node_num_accelerators(self) -> int:
        return self._num_accelerators

    def get_current_process_visible_accelerator_ids(self):
        if self._visible_ids is not None:
            return [str(i) for i in range(self._visible_ids)]
        return [str(i) for i in range(self._num_accelerators)]

    def get_resource_name(self) -> str:
        return self._resource_name

    def get_current_node_accelerator_type(self) -> str:
        return self._accelerator_type

    def get_visible_accelerator_ids_env_var(self) -> str:
        return "CUDA_VISIBLE_DEVICES"

    def get_current_node_additional_resources(self):
        return self._additional_resources or {}

    def set_current_process_visible_accelerator_ids(self, ids):
        pass

    def validate_resource_request_quantity(self, quantity: int) -> None:
        pass


def test_resource_and_label_spec_resolves_with_params():
    """Validate that ResourceAndLabelSpec resolve() respects passed in
    Ray Params rather than overriding with auto-detection/system defaults."""
    # Create ResourceAndLabelSpec with args from RayParams.
    spec = ResourceAndLabelSpec(
        num_cpus=8,
        num_gpus=2,
        memory=10 * 1024**3,
        object_store_memory=5 * 1024**3,
        resources={"TPU": 42},
        labels={"ray.io/market-type": "spot"},
    )

    spec.resolve(is_head=False)

    # Verify that explicit Ray Params values are preserved.
    assert spec.num_cpus == 8
    assert spec.num_gpus == 2
    assert spec.memory == 10 * 1024**3
    assert spec.object_store_memory == 5 * 1024**3
    assert spec.resources["TPU"] == 42
    assert any(key.startswith(NODE_ID_PREFIX) for key in spec.resources)
    assert spec.labels["ray.io/market-type"] == "spot"

    assert spec.resolved()


def test_resource_and_label_spec_resolves_auto_detect(monkeypatch):
    """Validate that ResourceAndLabelSpec resolve() fills out defaults detected from
    system when Params not passed."""
    monkeypatch.setattr("ray._private.utils.get_num_cpus", lambda: 4)  # 4 cpus
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory", lambda: 16 * 1024**3
    )  # 16GB
    monkeypatch.setattr(
        "ray._private.utils.estimate_available_memory", lambda: 8 * 1024**3
    )  # 8GB
    monkeypatch.setattr(
        "ray._private.utils.get_shared_memory_bytes", lambda: 4 * 1024**3
    )  # 4GB

    spec = ResourceAndLabelSpec()
    spec.resolve(is_head=True)

    assert spec.resolved()

    # Validate all fields are set based on defaults or calls to system.
    assert spec.num_cpus == 4
    assert spec.num_gpus == 0
    assert isinstance(spec.labels, dict)
    assert HEAD_NODE_RESOURCE_NAME in spec.resources
    assert any(key.startswith(NODE_ID_PREFIX) for key in spec.resources.keys())

    if sys.platform == "darwin":
        # Object store memory is capped at 2GB on macOS.
        expected_object_store = 2 * 1024**3
    else:
        # object_store_memory = 8GB * DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
        expected_object_store = int(
            8 * 1024**3 * ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
        )
    assert spec.object_store_memory == expected_object_store

    # memory is total available memory - object_store_memory
    expected_memory = 8 * 1024**3 - expected_object_store
    assert spec.memory == expected_memory


def test_env_resource_overrides_with_conflict(monkeypatch):
    """Validate that RESOURCES_ENVIRONMENT_VARIABLE overrides Ray Param resources."""
    # Prepare environment overrides
    env_resources = {
        "CPU": 8,
        "GPU": 4,
        "TPU": 4,
    }
    monkeypatch.setenv(
        ray_constants.RESOURCES_ENVIRONMENT_VARIABLE, json.dumps(env_resources)
    )

    ray_params_resources = {"TPU": 8, "B200": 4}

    # num_cpus, num_gpus, and conflicting resources should override
    spec = ResourceAndLabelSpec(
        num_cpus=2,
        num_gpus=1,
        resources=ray_params_resources,
        labels={},
    )

    spec.resolve(is_head=True)

    # Environment overrides values take precedence after resolve
    assert spec.num_cpus == 8
    assert spec.num_gpus == 4
    assert spec.resources["TPU"] == 4
    assert spec.resources["B200"] == 4


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


def test_resolve_memory_resources(monkeypatch):
    """Validate that resolve correctly sets system object_store memory and
    raises ValueError when configured memory is too low."""
    # object_store_memory capped at 95% of shm size to avoid low performance.
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory", lambda: 2 * 1024**3
    )  # 2 GB
    monkeypatch.setattr(
        "ray._private.utils.estimate_available_memory", lambda: 1 * 1024**3
    )  # 2 GB
    monkeypatch.setattr(
        "ray._private.utils.get_shared_memory_bytes", lambda: 512 * 1024**2
    )  # 512 MB

    spec1 = ResourceAndLabelSpec()
    spec1.resolve(is_head=False)

    max_shm = 512 * 1024**2 * 0.95
    assert spec1.object_store_memory <= max_shm
    assert spec1.memory > 0

    # Low available memory for tasks/actors triggers ValueError.
    monkeypatch.setattr(
        "ray._common.utils.get_system_memory", lambda: 2 * 1024**3
    )  # 2 GB
    monkeypatch.setattr(
        "ray._private.utils.estimate_available_memory", lambda: 100 * 1024**2
    )  # 100 MB
    monkeypatch.setattr(
        "ray._private.utils.get_shared_memory_bytes", lambda: 50 * 1024**2
    )  # 50 MB

    spec2 = ResourceAndLabelSpec()
    with pytest.raises(ValueError, match="available for tasks and actors"):
        spec2.resolve(is_head=False)


def test_resolve_raises_on_reserved_head_resource():
    """resolve should raise a ValueError if HEAD_NODE_RESOURCE_NAME is set in resources."""
    spec = ResourceAndLabelSpec(resources={HEAD_NODE_RESOURCE_NAME: 1}, labels={})
    with pytest.raises(ValueError, match=HEAD_NODE_RESOURCE_NAME):
        spec.resolve(is_head=True)


def test_resolve_handles_no_accelerators():
    """Check resolve() is able to handle the no accelerators detected case."""
    spec = ResourceAndLabelSpec()
    # No accelerators are returned.
    with patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=[],
    ):
        spec.resolve(is_head=False, node_ip_address="test")

    # With no accelerators detected or num_gpus, GPU count should default to 0
    # and the resources dictionary is unchanged.
    assert spec.num_gpus == 0
    assert spec.resources == {"node:test": 1}
    assert spec.resolved()


def test_label_spec_resolve_merged_env_labels(monkeypatch):
    """Validate that LABELS_ENVIRONMENT_VARIABLE is merged into final labels."""
    override_labels = {"autoscaler-override-label": "example"}
    monkeypatch.setenv(
        ray_constants.LABELS_ENVIRONMENT_VARIABLE, json.dumps(override_labels)
    )
    spec = ResourceAndLabelSpec()
    spec.resolve(is_head=True)

    assert any(key == "autoscaler-override-label" for key in spec.labels)


def test_merge_labels_populates_defaults(monkeypatch):
    """Ensure default labels (node type, market type, region, zone, accelerator) populate correctly."""
    # Patch Ray K8s label environment vars
    monkeypatch.setenv(ray_constants.LABELS_ENVIRONMENT_VARIABLE, "{}")
    monkeypatch.setenv("RAY_NODE_TYPE_NAME", "worker-group-1")
    monkeypatch.setenv("RAY_NODE_MARKET_TYPE", "spot")
    monkeypatch.setenv("RAY_NODE_REGION", "us-west1")
    monkeypatch.setenv("RAY_NODE_ZONE", "us-west1-a")

    spec = ResourceAndLabelSpec()

    # AcceleratorManager for node with 1 GPU
    with patch(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        return_value=FakeAcceleratorManager("GPU", "A100", 1),
    ), patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["GPU"],
    ):
        spec.resolve(is_head=False)

    # Verify all default labels are present
    assert spec.labels.get("ray.io/node-group") == "worker-group-1"
    assert spec.labels.get("ray.io/market-type") == "spot"
    assert spec.labels.get("ray.io/availability-region") == "us-west1"
    assert spec.labels.get("ray.io/availability-zone") == "us-west1-a"
    assert spec.labels.get("ray.io/accelerator-type") == "A100"
    assert spec.resolved()


def test_resolve_raises_if_exceeds_visible_devices():
    """Check that ValueError is raised when requested accelerators exceed visible IDs."""
    spec = ResourceAndLabelSpec()
    spec.num_gpus = 3  # request 3 GPUs

    with patch(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        return_value=FakeAcceleratorManager(
            "GPU", "A100", num_accelerators=5, visible_ids=2
        ),
    ), patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["GPU"],
    ):
        with pytest.raises(ValueError, match="Attempting to start raylet"):
            spec.resolve(is_head=False)


def test_resolve_sets_accelerator_resources():
    """Verify that GPUs/TPU values are auto-detected and assigned properly."""
    spec = ResourceAndLabelSpec()

    # Mock a node with GPUs with 4 visible IDs
    with patch(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        return_value=FakeAcceleratorManager("GPU", "A100", 4),
    ), patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["GPU"],
    ):
        spec.resolve(is_head=False)

    assert spec.num_gpus == 4
    assert spec.resources.get("accelerator_type:A100") == 1


def test_respect_configured_num_gpus():
    """Ensure manually set num_gpus overrides differing auto-detected accelerator value."""
    # Create a ResourceAndLabelSpec with num_gpus=2 from Ray Params.
    spec = ResourceAndLabelSpec(num_gpus=2)
    # Mock a node with GPUs with 4 visible IDs
    with patch(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        return_value=FakeAcceleratorManager("GPU", "A100", 4),
    ), patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["GPU"],
    ):
        spec.resolve(is_head=False)

    assert spec.num_gpus == 2, (
        f"Expected manually set num_gpus=2 to take precedence over auto-detected value, "
        f"but got {spec.num_gpus}"
    )
    # Accelerator type key should be set in resources.
    assert spec.resources.get("accelerator_type:A100") == 1


def test_resolve_sets_non_gpu_accelerator():
    """Verify that non-GPU accelerators are added to resources. Non-GPU accelerators
    should not alter the value of num_gpus."""
    spec = ResourceAndLabelSpec()
    # Mock accelerator manager to return a TPU v6e accelerator
    with patch(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        return_value=FakeAcceleratorManager("TPU", "TPU-v6e", 2, {"TPU-v6e-8-HEAD": 1}),
    ), patch(
        "ray._private.accelerators.get_all_accelerator_resource_names",
        return_value=["TPU"],
    ):
        spec.resolve(is_head=False)

    # num_gpus should default to 0
    assert spec.num_gpus == 0
    assert spec.resources["TPU"] == 2
    assert spec.resources["TPU-v6e-8-HEAD"] == 1
    # Accelerator type label is present
    assert spec.labels.get("ray.io/accelerator-type") == "TPU-v6e"
    assert spec.resolved()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
