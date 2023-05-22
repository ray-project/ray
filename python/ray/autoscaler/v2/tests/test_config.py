# coding: utf-8
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


def test_ray_start():
    raw_config = load_test_config("test_ray_up_config.yaml")
    config = NodeProviderConfig(raw_config)
    assert config.get_worker_start_ray_commands() == [
        "ray stop",
        "ray start --address=$RAY_HEAD_IP",
    ]
    assert config.get_worker_setup_commands("head_node") == [
        "echo a",
        "echo b",
        "echo ${echo hi}",
        "echo worker",
    ]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
