# coding: utf-8
import sys

import pytest  # noqa
import yaml
from ray._private.test_utils import load_test_config
from ray.autoscaler.v2.instance_manager.config import NodeProviderConfig


def test_simple():
    raw_config = load_test_config("test_multi_node.yaml")
    print(raw_config)
    config = NodeProviderConfig(raw_config)
    assert config.get_node_config("head_node") == {"InstanceType": "m5.large"}


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
