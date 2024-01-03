import os
import sys
import unittest
from collections import defaultdict

# coding: utf-8
from typing import Dict, Set
from unittest.mock import patch

import pytest

from mock import MagicMock

from ray.autoscaler.v2.instance_manager.instance_manager import (
    DefaultInstanceManager,
    InstanceUtil,
)
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage, StoreStatus
from ray.autoscaler.v2.schema import InvalidInstanceStatusError
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance,
    InstanceUpdateEvent,
    LaunchRequest,
    StatusCode,
    UpdateInstanceManagerStateRequest,
)


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
        with pytest.raises(InvalidInstanceStatusError):
            InstanceUtil.set_status(instance, Instance.RAY_RUNNING)

        with pytest.raises(InvalidInstanceStatusError):
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
        assert InstanceUtil.get_status_times_ns(instance, Instance.QUEUED)[0] == 1
        # No filter.
        assert InstanceUtil.get_status_times_ns(
            instance,
        ) == [1]

        # Missing status returns empty list
        assert InstanceUtil.get_status_times_ns(instance, Instance.REQUESTED) == []

        # Multiple status.
        mock_time.return_value = 2
        InstanceUtil.set_status(instance, Instance.REQUESTED)
        mock_time.return_value = 3
        InstanceUtil.set_status(instance, Instance.QUEUED)
        assert InstanceUtil.get_status_times_ns(instance, Instance.QUEUED) == [1, 3]

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
            assert InstanceUtil.is_cloud_instance_allocated(instance)
            all_status.remove(s)

        # Unknown not possible.
        all_status.remove(Instance.UNKNOWN)
        for s in all_status:
            instance.status = s
            assert not InstanceUtil.is_cloud_instance_allocated(instance)

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
            assert InstanceUtil.is_ray_running_reachable(instance)
            assert exists_path(
                s, Instance.RAY_RUNNING, InstanceUtil.get_valid_transitions()
            )
            all_status.remove(s)

        # Unknown not possible.
        all_status.remove(Instance.UNKNOWN)
        for s in all_status:
            instance.status = s
            assert not InstanceUtil.is_ray_running_reachable(instance)
            assert not exists_path(
                s, Instance.RAY_RUNNING, InstanceUtil.get_valid_transitions()
            )


class DefaultInstanceManagerTest(unittest.TestCase):
    def test_instances_version_mismatch(self):
        ins_storage = MagicMock()
        im = DefaultInstanceManager(ins_storage)
        # Version mismatch on reading from the storage.
        ins_storage.get_instances.return_value = ({}, 1)

        launch_req = LaunchRequest(
            instance_type="type-1",
            count=1,
            id="id-1",
        )
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,
                launch_requests=[launch_req],
            )
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH

        # Version OK.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                launch_requests=[launch_req],
            )
        )
        assert reply.status.code == StatusCode.OK

        # Version mismatch when writing to the storage (race happens)
        ins_storage.batch_upsert_instances.return_value = StoreStatus(
            False, 2  # No longer 1
        )
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                launch_requests=[launch_req],
            )
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH

        # Non-version mismatch error.
        ins_storage.batch_upsert_instances.return_value = StoreStatus(
            False, 1  # Still 1
        )
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                launch_requests=[launch_req],
            )
        )
        assert reply.status.code == StatusCode.UNKNOWN_ERRORS

    def test_get_and_updates(self):
        ins_storage = InstanceStorage(
            "cluster-id",
            InMemoryStorage(),
        )
        im = DefaultInstanceManager(ins_storage)

        # Empty storage.
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert reply.status.code == StatusCode.OK
        assert reply.state.instances == []

        # Launch nodes.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,
                launch_requests=[
                    LaunchRequest(
                        instance_type="type-1",
                        count=1,
                        id="id-1",
                    ),
                    LaunchRequest(
                        instance_type="type-2",
                        count=2,
                        id="id-2",
                    ),
                ],
            )
        )
        assert reply.status.code == StatusCode.OK

        # Get launched nodes.
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert reply.status.code == StatusCode.OK
        assert len(reply.state.instances) == 3

        types_count = defaultdict(int)
        for ins in reply.state.instances:
            types_count[ins.instance_type] += 1
            assert ins.status == Instance.QUEUED
            assert ins.launch_request_id in ["id-1", "id-2"]

        assert types_count["type-1"] == 1
        assert types_count["type-2"] == 2

        # Update node status.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="0",
                        new_instance_status=Instance.REQUESTED,
                    ),
                    InstanceUpdateEvent(
                        instance_id="1",
                        new_instance_status=Instance.REQUESTED,
                    ),
                ],
            )
        )

        assert reply.status.code == StatusCode.OK

        # Get updated nodes.
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert reply.status.code == StatusCode.OK
        assert len(reply.state.instances) == 3

        types_count = defaultdict(int)
        for ins in reply.state.instances:
            types_count[ins.instance_type] += 1
            if ins.instance_id in ["0", "1"]:
                assert ins.status == Instance.REQUESTED
            else:
                assert ins.status == Instance.QUEUED

        # Invalid instances status update.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=2,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="2",
                        new_instance_status=Instance.RAY_RUNNING,  # Not requested yet.
                    ),
                ],
            )
        )
        assert reply.status.code == StatusCode.INVALID_VALUE

        # Invalid versions.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,  # Invalid version, outdated.
                updates=[
                    InstanceUpdateEvent(
                        instance_id="2",
                        new_instance_status=Instance.REQUESTED,
                    ),
                ],
            )
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
