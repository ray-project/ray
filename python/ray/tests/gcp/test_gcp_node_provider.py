from typing import Dict
from threading import RLock
import pytest
from unittest.mock import MagicMock, patch, call

from ray.autoscaler._private.gcp.node import (
    GCPCompute,
    GCPNode,
    GCPNodeType,
    GCPResource,
)

from ray.tests.test_autoscaler import MockProcessRunner
from ray.autoscaler._private.gcp.node_provider import GCPNodeProvider
from ray.autoscaler._private.gcp.tpu_command_runner import (
    TPUCommandRunner,
    TPUVMSSHCommandRunner,
    TPUVMDockerCommandRunner,
)
from ray.autoscaler._private.command_runner import SSHCommandRunner, DockerCommandRunner

_PROJECT_NAME = "project-one"
_AZ = "us-west1-b"

auth_config = {
    "ssh_user": "ray",
    "ssh_private_key": "8265.pem",
}


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


def test_terminate_nodes():
    mock_node_config = {"machineType": "n2-standard-8"}
    node_type = GCPNodeType.COMPUTE.value
    id1, id2 = f"instance-id1-{node_type}", f"instance-id2-{node_type}"
    terminate_node_ids = [id1, id2]
    mock_resource = MagicMock()
    mock_resource.create_instances.return_value = [
        ({"dict": 1}, id1),
        ({"dict": 2}, id2),
    ]
    mock_resource.delete_instance.return_value = "test"

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.cached_nodes: Dict[str, GCPNode] = {}
        self.resources: Dict[GCPNodeType, GCPResource] = {}
        self.resources[GCPNodeType.COMPUTE] = mock_resource

    with patch.object(GCPNodeProvider, "__init__", __init__):
        node_provider = GCPNodeProvider({}, "")
        node_provider.create_node(mock_node_config, {}, 1)
        node_provider.terminate_nodes(terminate_node_ids)

    mock_resource.delete_instance.assert_has_calls(
        [call(node_id=id1), call(node_id=id2)], any_order=True
    )


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


def test_compute_node_list_instances_excludes_tpu():
    mock_execute = MagicMock(return_value={"test": "abc"})
    mock_list = MagicMock(return_value=MagicMock(execute=mock_execute))
    mock_instances = MagicMock(return_value=MagicMock(list=mock_list))
    mock_resource = MagicMock(instances=mock_instances)

    GCPCompute(mock_resource, _PROJECT_NAME, _AZ, "cluster_name").list_instances()
    filter_arg = mock_list.call_args.kwargs["filter"]

    # Checks that the tpu negation filter is included.
    assert "tpu_cores" in filter_arg


@pytest.mark.parametrize(
    "test_case",
    [
        (
            {},
            SSHCommandRunner,
        ),
        (
            {"docker_config": {"container_name": "container"}},
            DockerCommandRunner,
        ),
    ],
)
def test_cpu_resource_returns_standard_command_runner(test_case):
    mock_resource = MagicMock()

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.cached_nodes: Dict[str, GCPNode] = {}
        self.resources: Dict[GCPNodeType, GCPResource] = {}
        self.resources[GCPNodeType.COMPUTE] = mock_resource

    with patch.object(GCPNodeProvider, "__init__", __init__):
        node_provider = GCPNodeProvider({}, "")

    optional_docker_config, expected_runner = test_case

    args = {
        "log_prefix": "test",
        "node_id": "test-instance-compute",
        "auth_config": auth_config,
        "cluster_name": "test",
        "process_runner": MockProcessRunner(),
        "use_internal_ip": True,
    }
    args.update(optional_docker_config)
    command_runner = node_provider.get_command_runner(**args)
    assert isinstance(command_runner, expected_runner)


@pytest.mark.parametrize(
    "test_case",
    [
        (
            {},
            TPUVMSSHCommandRunner,
        ),
        (
            {"docker_config": {"container_name": "container"}},
            TPUVMDockerCommandRunner,
        ),
    ],
)
def test_tpu_resource_returns_tpu_command_runner(test_case):
    mock_resource = MagicMock()

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.cached_nodes: Dict[str, GCPNode] = {}
        self.resources: Dict[GCPNodeType, GCPResource] = {}
        self.resources[GCPNodeType.COMPUTE] = mock_resource
        self.resources[GCPNodeType.TPU] = mock_resource

    with patch.object(GCPNodeProvider, "__init__", __init__):
        node_provider = GCPNodeProvider({}, "")

    optional_docker_config, expected_runner = test_case

    args = {
        "log_prefix": "test",
        "node_id": "test-instance-tpu",
        "auth_config": auth_config,
        "cluster_name": "test",
        "process_runner": MockProcessRunner(),
        "use_internal_ip": True,
    }
    args.update(optional_docker_config)
    command_runner = node_provider.get_command_runner(**args)
    assert isinstance(command_runner, TPUCommandRunner)
    assert isinstance(command_runner._command_runners[0], expected_runner)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
