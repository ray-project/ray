from unittest.mock import patch

import pytest

import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray.serve._private.common import CreatePlacementGroupRequest
from ray.serve._private.default_impl import (
    _create_replica_placement_group,
    _ReplicaPlacementGroup,
)
from ray.serve.config import TPUAcceleratorConfig


@pytest.fixture
def mock_tpu_cluster():
    # Simulates a Ray cluster with a multi-host TPU v6e-16 slice (4x4 topology).
    pod_type = "v6e-16"
    topology = "4x4"
    cluster = Cluster()
    # Head node
    cluster.add_node(num_cpus=4)

    # TPU nodes: A 4x4 v6e slice has 16 chips. We simulate 4 hosts with 4 chips each.
    for i in range(4):
        env_vars = {
            "TPU_NAME": "test-slice",
            "TPU_WORKER_ID": str(i),
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        labels = {
            "ray.io/tpu-slice-name": "test-slice",
            "ray.io/tpu-worker-id": str(i),
            "ray.io/tpu-pod-type": pod_type,
        }
        resources = {"TPU": 4, "accelerator_type:TPU-V6E": 4}

        # The first node is the "head" of the slice
        if i == 0:
            resources[f"TPU-{pod_type}-head"] = 1

        cluster.add_node(
            num_cpus=8,
            resources=resources,
            labels=labels,
            env_vars=env_vars,
        )

    cluster.wait_for_nodes()
    ray.init(address=cluster.address, ignore_reinit_error=True)
    serve.start()
    yield cluster
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def test_tpu_accelerator_config_integration(mock_tpu_cluster):
    """Test that AcceleratorConfig correctly creates SlicePlacementGroup in a mock cluster."""

    tpu_config = TPUAcceleratorConfig(topology="4x4", accelerator_version="v6e")

    request = CreatePlacementGroupRequest(
        bundles=[{"CPU": 1}],  # Ignored since accel_config will override it
        strategy="SPREAD",
        target_node_id=None,
        name="test-tpu-pg",
        accelerator_config=tpu_config,
    )

    # This should call _create_tpu_placement_group and return a wrapper
    replica_pg = _create_replica_placement_group(request)

    assert isinstance(replica_pg, _ReplicaPlacementGroup)
    assert replica_pg._slice_pg is not None

    # Verify the placement group is ready
    ray.get(replica_pg.placement_group.ready(), timeout=20)

    # Verify cleanup
    replica_pg.shutdown()
    assert replica_pg._slice_pg is None


def test_tpu_accelerator_config_timeout_cleanup(mock_tpu_cluster):
    """Test that SlicePlacementGroup cleans up head PGs on timeout."""

    # Request a topology that requires 8 hosts (v6e-32) when cluster only has 4.
    tpu_config = TPUAcceleratorConfig(topology="4x8", accelerator_version="v6e")

    request = CreatePlacementGroupRequest(
        bundles=[{"CPU": 1}],
        strategy="SPREAD",
        target_node_id=None,
        name="test-tpu-timeout-pg",
        accelerator_config=tpu_config,
    )

    # Patch timeout to be short, and mock remove_placement_group to verify cleanup
    with patch("ray._private.accelerators.tpu.remove_placement_group") as mock_remove:
        with patch("ray.util.tpu.DEFAULT_TPU_HEAD_RESERVATION_TIMEOUT_S", 2.0):
            with pytest.raises(TimeoutError, match="Failed to reserve TPU head"):
                _create_replica_placement_group(request)

        assert mock_remove.called


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
