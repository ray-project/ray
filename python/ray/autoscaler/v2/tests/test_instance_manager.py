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
from ray.autoscaler.v2.tests.util import MockSubscriber
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance,
    InstanceUpdateEvent,
    StatusCode,
    UpdateInstanceManagerStateRequest,
)


class InstanceManagerTest(unittest.TestCase):
    def test_instances_version_mismatch(self):
        ins_storage = MagicMock()
        subscriber = MockSubscriber()
        im = InstanceManager(
            ins_storage, instance_status_update_subscribers=[subscriber]
        )
        # Version mismatch on reading from the storage.
        ins_storage.get_instances.return_value = ({}, 1)

        update = InstanceUpdateEvent(
            instance_id="id-1",
            new_instance_status=Instance.QUEUED,
            instance_type="type-1",
        )
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,
                updates=[update],
            )
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH
        assert len(subscriber.events) == 0

        # Version OK.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                updates=[update],
            )
        )
        assert reply.status.code == StatusCode.OK
        assert len(subscriber.events) == 1
        assert subscriber.events[0].new_instance_status == Instance.QUEUED

        # Version mismatch when writing to the storage (race happens)
        ins_storage.batch_upsert_instances.return_value = StoreStatus(
            False, 2  # No longer 1
        )
        subscriber.clear()
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                updates=[update],
            )
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH
        assert len(subscriber.events) == 0

        # Non-version mismatch error.
        ins_storage.batch_upsert_instances.return_value = StoreStatus(
            False, 1  # Still 1
        )
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                updates=[update],
            )
        )
        assert reply.status.code == StatusCode.UNKNOWN_ERRORS
        assert len(subscriber.events) == 0

    def test_get_and_updates(self):
        ins_storage = InstanceStorage(
            "cluster-id",
            InMemoryStorage(),
        )
        subscriber = MockSubscriber()
        im = InstanceManager(
            ins_storage, instance_status_update_subscribers=[subscriber]
        )

        # Empty storage.
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert reply.status.code == StatusCode.OK
        assert list(reply.state.instances) == []

        # Launch nodes.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,
                updates=[
                    InstanceUpdateEvent(
                        instance_type="type-1",
                        instance_id="id-1",
                        new_instance_status=Instance.QUEUED,
                    ),
                    InstanceUpdateEvent(
                        instance_type="type-2",
                        instance_id="id-2",
                        new_instance_status=Instance.QUEUED,
                    ),
                    InstanceUpdateEvent(
                        instance_type="type-2",
                        instance_id="id-3",
                        new_instance_status=Instance.QUEUED,
                    ),
                ],
            )
        )
        assert reply.status.code == StatusCode.OK
        assert len(subscriber.events) == 3
        for e in subscriber.events:
            assert e.new_instance_status == Instance.QUEUED

        # Get launched nodes.
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert reply.status.code == StatusCode.OK
        assert len(reply.state.instances) == 3

        instance_ids = [ins.instance_id for ins in reply.state.instances]

        types_count = defaultdict(int)
        for ins in reply.state.instances:
            types_count[ins.instance_type] += 1
            assert ins.status == Instance.QUEUED

        assert types_count["type-1"] == 1
        assert types_count["type-2"] == 2

        # Update node status.
        subscriber.clear()
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                updates=[
                    InstanceUpdateEvent(
                        instance_id=instance_ids[0],
                        new_instance_status=Instance.REQUESTED,
                        launch_request_id="l1",
                    ),
                    InstanceUpdateEvent(
                        instance_id=instance_ids[1],
                        new_instance_status=Instance.REQUESTED,
                        launch_request_id="l1",
                    ),
                ],
            )
        )

        assert reply.status.code == StatusCode.OK
        assert len(subscriber.events) == 2
        for e in subscriber.events:
            assert e.new_instance_status == Instance.REQUESTED

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
        subscriber.clear()
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
        assert len(subscriber.events) == 0

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
        assert len(subscriber.events) == 0


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
