import os
import sys
import pytest

from ray.cluster_utils import AutoscalingCluster

from ray.autoscaler._private.fake_multi_node.node_provider import (
    FAKE_HEAD_NODE_ID,
)
import ray


def test_basic():
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 4},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 30,
            },
        },
        idle_timeout_minutes=999,
    )
    cluster._config["provider"]["head_node_id"] = FAKE_HEAD_NODE_ID
    cluster._config["provider"]["launch_multiple"] = True

    cluster.start(
        override_env={
            "RAY_CLOUD_INSTANCE_ID": FAKE_HEAD_NODE_ID,
            "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID,
            "RAY_enable_autoscaler_v2": "1",
        },
    )
    ray.init("auto")


if __name__ == "__main__":

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
