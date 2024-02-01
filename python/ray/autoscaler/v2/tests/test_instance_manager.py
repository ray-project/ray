import os
import sys
import unittest
from collections import defaultdict

# coding: utf-8
import pytest

from mock import MagicMock

from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage, StoreStatus
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance,
    InstanceUpdateEvent,
    LaunchRequest,
    StatusCode,
    UpdateInstanceManagerStateRequest,
)


class InstanceManagerTest(unittest.TestCase):
    def test_instances_version_mismatch(self):
        ins_storage = MagicMock()
        im = InstanceManager(ins_storage)
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
        im = InstanceManager(ins_storage)

        # Empty storage.
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert reply.status.code == StatusCode.OK
        assert list(reply.state.instances) == []

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

        instance_ids = [ins.instance_id for ins in reply.state.instances]

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
                        instance_id=instance_ids[0],
                        new_instance_status=Instance.REQUESTED,
                    ),
                    InstanceUpdateEvent(
                        instance_id=instance_ids[1],
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
            if ins.instance_id in [instance_ids[0], instance_ids[1]]:
                assert ins.status == Instance.REQUESTED
            else:
                assert ins.status == Instance.QUEUED

        # Invalid instances status update.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=2,
                updates=[
                    InstanceUpdateEvent(
                        instance_id=instance_ids[2],
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
                        instance_id=instance_ids[2],
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
