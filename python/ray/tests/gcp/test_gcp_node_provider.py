from typing import Dict
from threading import RLock
import pytest
from unittest.mock import MagicMock, patch

from ray.autoscaler._private.gcp.node import (
    GCPCompute,
    GCPNode,
    GCPNodeType,
    GCPResource,
)

from python.ray.autoscaler._private.gcp.node_provider import GCPNodeProvider

_PROJECT_NAME = "project-one"
_AZ = "us-west1-b"


def test_create_node_returns_dict():
    mock_node_config = {"machineType": "n2-standard-8"}
    mock_results = [({"dict": 1}, "instance_id1"), ({"dict": 2}, "instance_id2")]
    mock_resource = MagicMock()
    mock_resource.create_instances.return_value = mock_results
    expected_return_value = {"instance_id1": {"dict": 1}, "instance_id2": {"dict": 2}}

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.cached_nodes: Dict[str, GCPNode] = {}
        self.resources: Dict[GCPNodeType, GCPResource] = {}
        self.resources[GCPNodeType.COMPUTE] = mock_resource

    with patch.object(GCPNodeProvider, "__init__", __init__):
        node_provider = GCPNodeProvider({}, "")
        create_node_return_value = node_provider.create_node(mock_node_config, {}, 1)
    assert create_node_return_value == expected_return_value


@pytest.mark.parametrize(
    "test_case",
    [
        ("n1-standard-4", f"zones/{_AZ}/machineTypes/n1-standard-4"),
        (
            f"zones/{_AZ}/machineTypes/n1-standard-4",
            f"zones/{_AZ}/machineTypes/n1-standard-4",
        ),
    ],
)
def test_convert_resources_to_urls_machine(test_case):
    gcp_compute = GCPCompute(None, _PROJECT_NAME, _AZ, "cluster_name")
    base_machine, result_machine = test_case
    modified_config = gcp_compute._convert_resources_to_urls(
        {"machineType": base_machine}
    )
    assert modified_config["machineType"] == result_machine


@pytest.mark.parametrize(
    "test_case",
    [
        (
            "nvidia-tesla-k80",
            f"projects/{_PROJECT_NAME}/zones/{_AZ}/acceleratorTypes/nvidia-tesla-k80",
        ),
        (
            f"projects/{_PROJECT_NAME}/zones/{_AZ}/acceleratorTypes/nvidia-tesla-k80",
            f"projects/{_PROJECT_NAME}/zones/{_AZ}/acceleratorTypes/nvidia-tesla-k80",
        ),
    ],
)
def test_convert_resources_to_urls_accelerators(test_case):
    gcp_compute = GCPCompute(None, _PROJECT_NAME, _AZ, "cluster_name")
    base_accel, result_accel = test_case

    base_config = {
        "machineType": "n1-standard-4",
        "guestAccelerators": [{"acceleratorCount": 1, "acceleratorType": base_accel}],
    }
    modified_config = gcp_compute._convert_resources_to_urls(base_config)

    assert modified_config["guestAccelerators"][0]["acceleratorType"] == result_accel


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
