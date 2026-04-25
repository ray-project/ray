import os
import sys
import time

import pytest

from ray.autoscaler.v2.schema import (
    AutoscalerInstance,
    ClusterStatus,
    IPPRGroupSpec,
    IPPRStatus,
)
from ray.core.generated.autoscaler_pb2 import NodeState, NodeStatus
from ray.core.generated.instance_manager_pb2 import Instance



def test_cluster_status_default_stats():
    status = ClusterStatus()

    assert status.active_nodes == []
    assert status.idle_nodes == []
    assert status.pending_launches == []
    assert status.failed_launches == []
    assert status.pending_nodes == []
    assert status.failed_nodes == []
    assert status.cluster_resource_usage == []
    assert status.stats.gcs_request_time_s == 0.0
    assert status.stats.request_ts_s is None

def _make_ippr_status() -> IPPRStatus:
    return IPPRStatus(
        cloud_instance_id="ray-worker-1",
        spec=IPPRGroupSpec(
            min_cpu=1.0,
            max_cpu=4.0,
            min_memory=2,
            max_memory=8,
            resize_timeout=10,
        ),
        current_cpu=1.0,
        current_memory=2,
        desired_cpu=1.0,
        desired_memory=2,
    )


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


def test_ippr_status_queue_resize_request():
    status = _make_ippr_status()
    status.resizing_at = 123
    status.k8s_resize_status = "deferred"
    status.k8s_resize_message = "pending"
    status.raylet_id = "abc"

    assert status.queue_resize_request(desired_cpu=2.0, desired_memory=4)
    assert status.desired_cpu == 2.0
    assert status.desired_memory == 4
    assert status.resizing_at is None
    assert status.k8s_resize_status == "new"
    assert status.k8s_resize_message is None

    assert not status.queue_resize_request(desired_cpu=2.0, desired_memory=4)


def test_ippr_status_request_and_progress_helpers():
    status = _make_ippr_status()
    assert status.has_resize_request_to_send() is False
    assert status.is_in_progress() is False
    status.raylet_id = "abc"
    assert status.is_k8s_resize_finished()
    assert not status.has_resize_request_to_send()
    assert not status.is_in_progress()

    status.queue_resize_request(desired_cpu=2.0)
    assert status.has_resize_request_to_send()
    assert status.is_in_progress()
    assert not status.is_k8s_resize_finished()

    status.resizing_at = int(time.time())
    status.k8s_resize_status = None
    assert status.is_in_progress()
    assert status.is_k8s_resize_finished()


def test_ippr_status_need_sync_with_raylet():
    status = _make_ippr_status()
    assert status.need_sync_with_raylet() is False
    status.raylet_id = "abc"
    status.resizing_at = int(time.time())
    status.k8s_resize_status = None
    assert status.need_sync_with_raylet()

    status.desired_cpu = 2.0
    assert not status.need_sync_with_raylet()


def test_ippr_status_limits_and_can_resize_up():
    status = _make_ippr_status()
    assert status.can_resize_up() is False
    status.raylet_id = "abc"
    assert status.max_cpu() == 4.0
    assert status.max_memory() == 8
    assert status.can_resize_up()

    status.suggested_max_cpu = 1.5
    status.suggested_max_memory = 3
    assert status.max_cpu() == 1.5
    assert status.max_memory() == 3

    status.current_cpu = 1.5
    status.current_memory = 3
    assert not status.can_resize_up()

    status.current_cpu = 1.0
    status.last_failed_at = 1
    assert not status.can_resize_up()


def test_ippr_status_failure_and_timeout_helpers():
    status = _make_ippr_status()
    status.resizing_at = int(time.time()) - 20
    assert status.is_timeout()

    status.k8s_resize_status = "error"
    assert status.is_errored()

    status.record_failure("resize failed", failed_at=123)
    assert status.last_failed_at == 123
    assert status.last_failed_reason == "resize failed"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
