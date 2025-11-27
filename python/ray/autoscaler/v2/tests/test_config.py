# coding: utf-8
import os
import sys
import tempfile

import pytest  # noqa

from ray._common.utils import binary_to_hex
from ray._private.test_utils import get_test_config_path
from ray.autoscaler import AUTOSCALER_DIR_PATH
from ray.autoscaler._private.util import format_readonly_node_type
from ray.autoscaler.v2.instance_manager import config as config_mod
from ray.autoscaler.v2.instance_manager.config import (
    FileConfigReader,
    Provider,
    ReadOnlyProviderConfigReader,
)


@pytest.mark.parametrize(
    "skip_hash",
    [True, False],
)
def test_simple(skip_hash):
    config = FileConfigReader(
        get_test_config_path("test_multi_node.yaml"), skip_content_hash=skip_hash
    ).get_cached_autoscaling_config()
    assert config.get_cloud_node_config("head_node") == {"InstanceType": "m5.large"}
    assert config.get_docker_config("head_node") == {
        "image": "anyscale/ray-ml:latest",
        "container_name": "ray_container",
        "pull_before_run": True,
    }
    assert config.get_worker_start_ray_commands


def test_complex():
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()
    assert config.get_head_setup_commands() == [
        "echo a",
        "echo b",
        "echo ${echo hi}",
        "echo head",
    ]
    assert config.get_head_start_ray_commands() == [
        "ray stop",
        "ray start --head --autoscaling-config=~/ray_bootstrap_config.yaml",
    ]
    assert config.get_worker_setup_commands("worker_nodes") == [
        "echo a",
        "echo b",
        "echo ${echo hi}",
        "echo worker",
    ]
    assert config.get_worker_start_ray_commands() == [
        "ray stop",
        "ray start --address=$RAY_HEAD_IP",
    ]
    assert config.get_worker_setup_commands("worker_nodes1") == [
        "echo worker1",
    ]

    assert config.get_docker_config("head_node") == {
        "image": "anyscale/ray-ml:head-default",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("default") == {
        "image": "anyscale/ray-ml:worker-default",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("worker_nodes") == {
        "image": "anyscale/ray-ml:worker-default",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("worker_nodes1") == {
        "image": "anyscale/ray-ml:worker_nodes1",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_initialization_commands("worker_nodes") == ["echo what"]

    assert config.get_initialization_commands("worker_nodes1") == ["echo init"]

    assert config.get_node_resources("worker_nodes1") == {"CPU": 2}

    assert config.get_node_resources("worker_nodes") == {}

    assert config.get_node_labels("worker_nodes1") == {"foo": "bar"}

    assert config.get_config("cluster_name") == "test-cli"
    assert config.get_config("non-existing", "default") == "default"
    assert config.get_config("non-existing") is None


def test_multi_provider_instance_type():
    def load_config(file):
        path = os.path.join(AUTOSCALER_DIR_PATH, file)
        return FileConfigReader(path).get_cached_autoscaling_config()

    aws_config = load_config("aws/defaults.yaml")
    assert aws_config.get_provider_instance_type("ray.head.default") == "m5.large"

    gcp_config = load_config("gcp/defaults.yaml")
    # NOTE: Why is this underscore....
    assert gcp_config.get_provider_instance_type("ray_head_default") == "n1-standard-2"

    aliyun_config = load_config("aliyun/defaults.yaml")
    assert (
        aliyun_config.get_provider_instance_type("ray.head.default") == "ecs.n4.large"
    )

    azure_config = load_config("azure/defaults.yaml")
    assert (
        azure_config.get_provider_instance_type("ray.head.default") == "Standard_D2s_v3"
    )

    # TODO(rickyx):
    # We don't have kuberay and local config yet.


def test_node_type_configs():
    config = FileConfigReader(
        get_test_config_path("test_ray_complex.yaml")
    ).get_cached_autoscaling_config()

    node_type_configs = config.get_node_type_configs()
    assert config.get_max_num_worker_nodes() == 10
    assert len(node_type_configs) == 4
    assert node_type_configs["head_node"].max_worker_nodes == 1
    assert node_type_configs["head_node"].min_worker_nodes == 0
    assert node_type_configs["head_node"].resources == {}
    assert node_type_configs["head_node"].labels == {}

    assert node_type_configs["default"].max_worker_nodes == 2
    assert node_type_configs["default"].min_worker_nodes == 0
    assert node_type_configs["default"].resources == {}
    assert node_type_configs["default"].labels == {}

    assert node_type_configs["worker_nodes"].max_worker_nodes == 2
    assert node_type_configs["worker_nodes"].min_worker_nodes == 1
    assert node_type_configs["worker_nodes"].resources == {}
    assert node_type_configs["worker_nodes"].labels == {}

    assert node_type_configs["worker_nodes1"].max_worker_nodes == 2
    assert node_type_configs["worker_nodes1"].min_worker_nodes == 1
    assert node_type_configs["worker_nodes1"].resources == {"CPU": 2}
    assert node_type_configs["worker_nodes1"].labels == {"foo": "bar"}


def test_read_config():
    # Make a temp config file from aws/defaults.yaml
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        # Write "aws/defaults.yaml" to the temp file
        with open(
            os.path.join(AUTOSCALER_DIR_PATH, "aws/defaults.yaml"), "r"
        ) as default_file:
            f.write(default_file.read())

    config_reader = FileConfigReader(f.name)

    # Check that the config is read correctly
    assert config_reader.get_cached_autoscaling_config().provider == Provider.AWS

    # Now override the file with a different provider
    with open(f.name, "w") as f:
        # Replace the file with "gcp/defaults.yaml"
        with open(
            os.path.join(AUTOSCALER_DIR_PATH, "gcp/defaults.yaml"), "r"
        ) as default_file:
            f.write(default_file.read())

    # Still the same.
    assert config_reader.get_cached_autoscaling_config().provider == Provider.AWS

    # Reload
    config_reader.refresh_cached_autoscaling_config()
    assert config_reader.get_cached_autoscaling_config().provider == Provider.GCP


def test_readonly_node_type_name_and_fallback(monkeypatch):
    class _DummyNodeState:
        def __init__(self, ray_node_type_name, node_id, total_resources):
            self.ray_node_type_name = ray_node_type_name
            self.node_id = node_id
            self.total_resources = total_resources

    class _DummyClusterState:
        def __init__(self, node_states):
            self.node_states = node_states

    # Avoid real GCS usage.
    monkeypatch.setattr(config_mod, "GcsClient", lambda address: object())
    # Build a cluster with:
    # - 1 named head type
    # - 2 named worker types of the same type (aggregation check)
    # - 1 worker type without name (fallback to node_id-based type)
    unnamed_worker_id = b"\xab"
    fallback_name = format_readonly_node_type(binary_to_hex(unnamed_worker_id))
    nodes = [
        _DummyNodeState(
            "ray.head.default", b"\x01", {"CPU": 1, "node:__internal_head__": 1}
        ),
        _DummyNodeState("worker.custom", b"\x02", {"CPU": 2}),
        _DummyNodeState("worker.custom", b"\x03", {"CPU": 2}),
        _DummyNodeState("", unnamed_worker_id, {"CPU": 3}),
    ]
    monkeypatch.setattr(
        config_mod,
        "get_cluster_resource_state",
        lambda _gc: _DummyClusterState(nodes),
    )

    reader = ReadOnlyProviderConfigReader("dummy:0")
    reader.refresh_cached_autoscaling_config()
    cfg = reader.get_cached_autoscaling_config()

    node_types = cfg.get_config("available_node_types")
    # Head assertions
    assert "ray.head.default" in node_types
    assert node_types["ray.head.default"]["max_workers"] == 0
    assert cfg.get_head_node_type() == "ray.head.default"
    # Preferred name aggregation
    assert "worker.custom" in node_types
    assert node_types["worker.custom"]["max_workers"] == 2
    # Fallback for unnamed worker
    assert fallback_name in node_types
    assert node_types[fallback_name]["max_workers"] == 1


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
