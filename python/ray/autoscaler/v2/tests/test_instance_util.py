import os
import sys
import unittest

# coding: utf-8
from typing import Dict, Set
from unittest.mock import patch

import pytest

from ray.autoscaler.v2.instance_manager.common import (
    InstanceUtil,
    InvalidInstanceStatusTransitionError,
)
from ray.core.generated.instance_manager_pb2 import Instance


def exists_path(
    src: Instance.InstanceStatus,
    dst: Instance.InstanceStatus,
    graphs: Dict["Instance.InstanceStatus", Set["Instance.InstanceStatus"]],
) -> bool:
    # BFS search from src to dst to see if there is a path
    # from src to dst. There's no path if src == dst.
    visited = set()
    queue = [src]
    if src == dst:
        return False
    while queue:
        node = queue.pop(0)
        if node not in visited:
            visited.add(node)
            queue.extend(graphs[node])
    return dst in visited


class InstanceUtilTest(unittest.TestCase):
    def test_basic(self):
        # New instance.
        instance = InstanceUtil.new_instance("i-123", "type_1", "rq-1")
        assert instance.instance_id == "i-123"
        assert instance.instance_type == "type_1"
        assert instance.launch_request_id == "rq-1"
        assert instance.status == Instance.QUEUED

        # Set status.
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        assert instance.status == Instance.REQUESTED

        # Set status with invalid status.
        with pytest.raises(InvalidInstanceStatusTransitionError):
            InstanceUtil.set_status(instance, Instance.RAY_RUNNING)

        with pytest.raises(InvalidInstanceStatusTransitionError):
            InstanceUtil.set_status(instance, Instance.UNKNOWN)

    def test_transition_graph(self):
        # Assert on each edge in the graph.
        all_status = set(Instance.InstanceStatus.values())

        g = InstanceUtil.get_valid_transitions()

        assert g[Instance.QUEUED] == {Instance.REQUESTED}
        all_status.remove(Instance.QUEUED)

        assert g[Instance.REQUESTED] == {
            Instance.ALLOCATED,
            Instance.QUEUED,
            Instance.ALLOCATION_FAILED,
        }
        all_status.remove(Instance.REQUESTED)

        assert g[Instance.ALLOCATED] == {
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.STOPPING,
            Instance.STOPPED,
        }
        all_status.remove(Instance.ALLOCATED)

        assert g[Instance.RAY_INSTALLING] == {
            Instance.RAY_RUNNING,
            Instance.RAY_INSTALL_FAILED,
            Instance.RAY_STOPPED,
            Instance.STOPPED,
        }
        all_status.remove(Instance.RAY_INSTALLING)

        assert g[Instance.RAY_RUNNING] == {
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.STOPPED,
        }
        all_status.remove(Instance.RAY_RUNNING)

        assert g[Instance.RAY_STOPPING] == {Instance.RAY_STOPPED, Instance.STOPPED}
        all_status.remove(Instance.RAY_STOPPING)

        assert g[Instance.RAY_STOPPED] == {Instance.STOPPED, Instance.STOPPING}
        all_status.remove(Instance.RAY_STOPPED)

        assert g[Instance.STOPPING] == {Instance.STOPPED}
        all_status.remove(Instance.STOPPING)

        assert g[Instance.STOPPED] == set()
        all_status.remove(Instance.STOPPED)

        assert g[Instance.ALLOCATION_FAILED] == set()
        all_status.remove(Instance.ALLOCATION_FAILED)

        assert g[Instance.RAY_INSTALL_FAILED] == {Instance.STOPPED, Instance.STOPPING}
        all_status.remove(Instance.RAY_INSTALL_FAILED)

        assert g[Instance.UNKNOWN] == set()
        all_status.remove(Instance.UNKNOWN)

        assert len(all_status) == 0

    @patch("time.time_ns")
    def test_status_time(self, mock_time):
        mock_time.return_value = 1
        instance = InstanceUtil.new_instance("i-123", "type_1", "rq-1")
        # OK
        assert (
            InstanceUtil.get_status_transition_times_ns(instance, Instance.QUEUED)[0]
            == 1
        )
        # No filter.
        assert InstanceUtil.get_status_transition_times_ns(
            instance,
        ) == [1]

        # Missing status returns empty list
        assert (
            InstanceUtil.get_status_transition_times_ns(instance, Instance.REQUESTED)
            == []
        )

        # Multiple status.
        mock_time.return_value = 2
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        mock_time.return_value = 3
        InstanceUtil.set_status(instance, Instance.QUEUED)
        mock_time.return_value = 4
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        assert InstanceUtil.get_status_transition_times_ns(
            instance, Instance.QUEUED
        ) == [1, 3]

    def test_is_cloud_instance_allocated(self):
        all_status = set(Instance.InstanceStatus.values())
        instance = InstanceUtil.new_instance("i-123", "type_1", "rq-1")
        positive_status = {
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_INSTALL_FAILED,
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPING,
            Instance.RAY_STOPPED,
            Instance.STOPPING,
        }
        for s in positive_status:
            instance.status = s
            assert InstanceUtil.is_cloud_instance_allocated(instance.status)
            all_status.remove(s)

        # Unknown not possible.
        all_status.remove(Instance.UNKNOWN)
        for s in all_status:
            instance.status = s
            assert not InstanceUtil.is_cloud_instance_allocated(instance.status)

    def test_is_ray_running_reachable(self):
        all_status = set(Instance.InstanceStatus.values())
        instance = InstanceUtil.new_instance("i-123", "type_1", "rq-1")
        positive_status = {
            Instance.QUEUED,
            Instance.REQUESTED,
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
        }
        for s in positive_status:
            instance.status = s
            assert InstanceUtil.is_ray_running_reachable(instance.status)
            assert exists_path(
                s, Instance.RAY_RUNNING, InstanceUtil.get_valid_transitions()
            )
            all_status.remove(s)

        # Unknown not possible.
        all_status.remove(Instance.UNKNOWN)
        for s in all_status:
            instance.status = s
            assert not InstanceUtil.is_ray_running_reachable(instance.status)
            assert not exists_path(
                s, Instance.RAY_RUNNING, InstanceUtil.get_valid_transitions()
            )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
