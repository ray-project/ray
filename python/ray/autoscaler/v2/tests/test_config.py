# coding: utf-8
import os
import sys

import pytest  # noqa
import yaml

from ray._private.test_utils import load_test_config
from ray.autoscaler import AUTOSCALER_DIR_PATH
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig


def test_simple():
    raw_config = load_test_config("test_multi_node.yaml")
    config = NodeProviderConfig(raw_config)
    assert config.get_node_config("head_node") == {"InstanceType": "m5.large"}
    assert config.get_docker_config("head_node") == {
        "image": "anyscale/ray-ml:latest",
        "container_name": "ray_container",
        "pull_before_run": True,
    }
    assert config.get_worker_start_ray_commands


def test_complex():
    raw_config = load_test_config("test_ray_complex.yaml")
    config = NodeProviderConfig(raw_config)
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
        "image": "anyscale/ray-ml:latest",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("worker_nodes") == {
        "image": "anyscale/ray-ml:latest",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_docker_config("worker_nodes1") == {
        "image": "anyscale/ray-ml:nightly",
        "container_name": "ray_container",
        "pull_before_run": True,
    }

    assert config.get_node_type_specific_config(
        "worker_nodes", "initialization_commands"
    ) == ["echo what"]

    assert config.get_node_type_specific_config(
        "worker_nodes1", "initialization_commands"
    ) == ["echo init"]

    assert config.get_node_resources("worker_nodes1") == {"CPU": 2}

    assert config.get_node_resources("worker_nodes") == {}


def test_multi_provider_instance_type():
    def load_config(file):
        path = os.path.join(AUTOSCALER_DIR_PATH, file)
        return NodeProviderConfig(
            yaml.safe_load(open(path).read()), skip_content_hash=True
        )

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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
