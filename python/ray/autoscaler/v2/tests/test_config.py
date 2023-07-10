# coding: utf-8
import os
import sys

import pytest  # noqa

from ray._private.test_utils import load_test_config
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


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
