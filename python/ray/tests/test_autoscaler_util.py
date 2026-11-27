import sys
from unittest import mock

import pytest

from ray.autoscaler._private import constants
from ray.autoscaler._private.node_tracker import NodeTracker
from ray.autoscaler._private.util import with_envs, with_head_node_ip


def test_with_envs():
    assert with_envs(
        ["echo $RAY_HEAD_IP", "ray start"], {"RAY_HEAD_IP": "127.0.0.0"}
    ) == [
        "export RAY_HEAD_IP=127.0.0.0; echo $RAY_HEAD_IP",
        "export RAY_HEAD_IP=127.0.0.0; ray start",
    ]

    assert with_head_node_ip(["echo $RAY_HEAD_IP"], "127.0.0.0") == [
        "export RAY_HEAD_IP=127.0.0.0; echo $RAY_HEAD_IP"
    ]

    assert (
        "export RAY_HEAD_IP=456"
        in with_envs(
            ["echo $RAY_CLOUD_ID"], {"RAY_CLOUD_ID": "123", "RAY_HEAD_IP": "456"}
        )[0]
    )
    assert (
        "export RAY_CLOUD_ID=123"
        in with_envs(
            ["echo $RAY_CLOUD_ID"], {"RAY_CLOUD_ID": "123", "RAY_HEAD_IP": "456"}
        )[0]
    )


def test_node_tracker_lru_eviction():
    tracker = NodeTracker()
    with mock.patch.object(constants, "AUTOSCALER_MAX_NODES_TRACKED", 2):
        tracker.track("node-1", "10.0.0.1", "worker")
        tracker.track("node-2", "10.0.0.2", "worker")
        tracker.track("node-3", "10.0.0.3", "worker")

    assert "node-1" not in tracker.node_mapping
    assert tracker.node_mapping == {
        "node-2": ("10.0.0.2", "worker"),
        "node-3": ("10.0.0.3", "worker"),
    }
    assert tracker.lru_order == ["node-2", "node-3"]
    assert tracker.get_all_failed_node_info({"node-2", "node-3"}) == []
    assert tracker.get_all_failed_node_info({"node-2"}) == [("10.0.0.3", "worker")]

    tracker.untrack("node-3")
    assert tracker.node_mapping == {"node-2": ("10.0.0.2", "worker")}
    assert tracker.lru_order == ["node-2"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
