import os
import sys

import pytest

from ray.autoscaler.v2.schema import AutoscalerInstance
from ray.core.generated.autoscaler_pb2 import NodeState, NodeStatus
from ray.core.generated.instance_manager_pb2 import Instance


def test_autoscaler_instance():

    i = AutoscalerInstance()
    assert not i.validate()[0], "Empty instance should be invalid"

    i = AutoscalerInstance(im_instance=Instance(status=Instance.QUEUED))
    assert i.validate()[0], "Instance with only im_instance should be valid"

    i = AutoscalerInstance(
        ray_node=NodeState(status=NodeStatus.RUNNING, instance_id="i-123"),
        cloud_instance_id="i-123",
    )
    assert i.validate()[0], i.validate()[1]

    i = AutoscalerInstance(
        ray_node=NodeState(status=NodeStatus.RUNNING),
    )
    assert not i.validate()[0], "Missing cloud node id."

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.QUEUED),
        ray_node=NodeState(status=NodeStatus.RUNNING),
    )
    assert not i.validate()[
        0
    ], "cloud node id is required to link the ray node with im state"

    i = AutoscalerInstance(
        cloud_instance_id="i-123",
    )
    assert not i.validate()[
        0
    ], "cloud instance id is not possible without im or ray node"

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.ALLOCATED, cloud_instance_id="i-123"),
        cloud_instance_id="i-123",
    )
    assert i.validate()[0]

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.ALLOCATED, cloud_instance_id="i-123"),
        cloud_instance_id="i-124",  # mismatch.
    )
    assert not i.validate()[0], "cloud instance id should match"

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.QUEUED, cloud_instance_id="i-123"),
        cloud_instance_id="i-123",
    )
    assert not i.validate()[0], "cloud instance id is not possible with queued state"

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.ALLOCATED, cloud_instance_id="i-123"),
    )
    assert not i.validate()[0], "cloud instance id should also be set"

    i = AutoscalerInstance(
        ray_node=NodeState(status=NodeStatus.RUNNING, instance_id="i-123"),
        cloud_instance_id="i-123",
    )
    assert i.validate()[0]

    i = AutoscalerInstance(
        ray_node=NodeState(status=NodeStatus.RUNNING, instance_id="i-123"),
        cloud_instance_id="i-124",  # mismatch.
    )
    assert not i.validate()[0]

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.RAY_RUNNING, cloud_instance_id="i-123"),
        ray_node=NodeState(status=NodeStatus.RUNNING, instance_id="i-123"),
        cloud_instance_id="i-123",
    )
    assert i.validate()[0]

    i = AutoscalerInstance(
        im_instance=Instance(status=Instance.RAY_RUNNING, cloud_instance_id="i-123"),
        ray_node=NodeState(status=NodeStatus.RUNNING, instance_id="i-123"),
        cloud_instance_id="i-124",  # mismatch.
    )
    assert not i.validate()[0]


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
