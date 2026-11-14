import os
import sys
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

# coding: utf-8
import pytest

from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage, StoreStatus
from ray.autoscaler.v2.tests.util import MockSubscriber
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance,
    InstanceUpdateEvent,
    NodeKind,
    StatusCode,
    UpdateInstanceManagerStateRequest,
)


class InstanceManagerTest(unittest.TestCase):
    @staticmethod
    def _update_instance_status(im, status):
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=-1,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="id-1",
                        new_instance_status=status,
                        instance_type="type-1",
                        launch_request_id="request-1",
                        upsert=True,
                    )
                ],
            )
        )
        assert reply.status.code == StatusCode.OK
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        return reply.state.instances[0]

    @patch("ray.autoscaler.v2.instance_manager.common.MAX_STATUS_HISTORY_LENGTH", 10)
    @patch("time.time_ns")
    def test_status_summary_survives_storage_round_trip(self, mock_time):
        storage = InstanceStorage("cluster-id", InMemoryStorage())
        im = InstanceManager(storage, [])
        mock_time.return_value = 0
        self._update_instance_status(im, Instance.QUEUED)
        for attempt in range(100):
            mock_time.return_value = 2 * attempt + 1
            self._update_instance_status(im, Instance.REQUESTED)
            mock_time.return_value += 1
            self._update_instance_status(im, Instance.QUEUED)

        # Every read creates a new protobuf object from serialized storage.
        instances, _ = storage.get_instances()
        instance = instances["id-1"]
        assert len(instance.status_history) == 10
        assert im.get_status_transition_count(instance, Instance.REQUESTED) == 100
        assert im.get_status_transition_count(instance, Instance.QUEUED) == 101
        assert im.get_first_status_transition_time_ns(instance, Instance.QUEUED) == 0
        assert im.get_first_status_transition_time_ns(instance, Instance.REQUESTED) == 1
        assert im.get_status_transition_count(instance, Instance.UNKNOWN) == 0
        assert (
            im.get_first_status_transition_time_ns(instance, Instance.UNKNOWN) is None
        )

        # Summaries are scoped to the manager, even with the same instance id.
        other = InstanceManager(InstanceStorage("other-cluster", InMemoryStorage()), [])
        mock_time.return_value = 300
        other_instance = self._update_instance_status(other, Instance.QUEUED)
        assert (
            other.get_status_transition_count(other_instance, Instance.REQUESTED) == 0
        )
        assert (
            other.get_first_status_transition_time_ns(other_instance, Instance.QUEUED)
            == 300
        )
        assert im.get_status_transition_count(instance, Instance.REQUESTED) == 100

        # A failed deletion must not discard counts already trimmed from history.
        assert not im.delete_instances(["id-1"], expected_storage_version=0).success
        assert im.get_status_transition_count(instance, Instance.REQUESTED) == 100
        assert im.delete_instances(
            ["id-1"], expected_storage_version=instance.version
        ).success
        assert not im._status_summaries
        assert not storage.get_instances()[0]
        recreated = self._update_instance_status(im, Instance.QUEUED)
        assert im.get_status_transition_count(recreated, Instance.REQUESTED) == 0
        assert im.get_first_status_transition_time_ns(recreated, Instance.QUEUED) == 300

    @patch("ray.autoscaler.v2.instance_manager.common.MAX_STATUS_HISTORY_LENGTH", 10)
    @patch("time.time_ns", return_value=201)
    def test_initialize_status_summary_from_existing_history(self, mock_time):
        for read_before_update in (False, True):
            with self.subTest(read_before_update=read_before_update):
                storage = InstanceStorage("cluster-id", InMemoryStorage())
                im = InstanceManager(storage, [])
                instance = Instance(
                    instance_id="id-1",
                    status=Instance.QUEUED,
                    status_history=[
                        Instance.StatusHistory(
                            instance_status=(
                                Instance.QUEUED if i % 2 == 0 else Instance.REQUESTED
                            ),
                            timestamp_ns=i,
                        )
                        for i in range(201)
                    ],
                )
                assert storage.upsert_instance(instance).success
                if read_before_update:
                    assert (
                        im.get_status_transition_count(instance, Instance.REQUESTED)
                        == 100
                    )
                    assert (
                        im.get_first_status_transition_time_ns(
                            instance, Instance.QUEUED
                        )
                        == 0
                    )

                instance = self._update_instance_status(im, Instance.REQUESTED)
                assert len(instance.status_history) == 10
                assert (
                    im.get_status_transition_count(instance, Instance.REQUESTED) == 101
                )
                assert im.get_status_transition_count(instance, Instance.QUEUED) == 101
                assert (
                    im.get_first_status_transition_time_ns(instance, Instance.QUEUED)
                    == 0
                )
                assert (
                    im.get_first_status_transition_time_ns(instance, Instance.REQUESTED)
                    == 1
                )

    @patch("time.time_ns")
    def test_failed_updates_do_not_change_status_summary(self, mock_time):
        storage = InstanceStorage("cluster-id", InMemoryStorage())
        subscriber = MockSubscriber()
        im = InstanceManager(storage, [subscriber])
        mock_time.return_value = 0
        instance = self._update_instance_status(im, Instance.QUEUED)
        subscriber.clear()
        mock_time.return_value = 1
        updates = [
            InstanceUpdateEvent(
                instance_id="id-1",
                new_instance_status=Instance.REQUESTED,
                launch_request_id="request-1",
                instance_type="type-1",
            ),
            InstanceUpdateEvent(
                instance_id="id-2",
                new_instance_status=Instance.QUEUED,
                instance_type="type-1",
                upsert=True,
            ),
        ]
        # Both read-time and write-time failures must leave summaries unchanged.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(expected_version=0, updates=updates)
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH
        for failed_version, expected_code in (
            (instance.version + 1, StatusCode.VERSION_MISMATCH),
            (instance.version, StatusCode.UNKNOWN_ERRORS),
        ):
            with patch.object(
                storage,
                "batch_upsert_instances",
                return_value=StoreStatus(False, failed_version),
            ):
                reply = im.update_instance_manager_state(
                    UpdateInstanceManagerStateRequest(
                        expected_version=instance.version, updates=updates
                    )
                )
            assert reply.status.code == expected_code
            assert im.get_status_transition_count(instance, Instance.REQUESTED) == 0
            assert (
                im.get_first_status_transition_time_ns(instance, Instance.REQUESTED)
                is None
            )
            assert im.get_status_transition_count(instance, Instance.QUEUED) == 1
            assert (
                im.get_first_status_transition_time_ns(instance, Instance.QUEUED) == 0
            )
            assert "id-2" not in im._status_summaries
            assert not subscriber.events

        # Retrying the update records exactly one successful request.
        instance = self._update_instance_status(im, Instance.REQUESTED)
        assert im.get_status_transition_count(instance, Instance.REQUESTED) == 1
        assert im.get_first_status_transition_time_ns(instance, Instance.REQUESTED) == 1

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
            upsert=True,
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
                        upsert=True,
                    ),
                    InstanceUpdateEvent(
                        instance_type="type-2",
                        instance_id="id-2",
                        new_instance_status=Instance.QUEUED,
                        upsert=True,
                    ),
                    InstanceUpdateEvent(
                        instance_type="type-2",
                        instance_id="id-3",
                        new_instance_status=Instance.QUEUED,
                        upsert=True,
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
                        instance_type="type-1",
                        launch_request_id="l1",
                    ),
                    InstanceUpdateEvent(
                        instance_id=instance_ids[1],
                        new_instance_status=Instance.REQUESTED,
                        launch_request_id="l1",
                        instance_type="type-1",
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
        with pytest.raises(AssertionError):
            reply = im.update_instance_manager_state(
                UpdateInstanceManagerStateRequest(
                    expected_version=2,
                    updates=[
                        InstanceUpdateEvent(
                            instance_id=instance_ids[2],
                            # Not requested yet.
                            new_instance_status=Instance.RAY_RUNNING,
                        ),
                    ],
                )
            )
        assert len(subscriber.events) == 0

        # Invalid versions.
        reply = im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,  # Invalid version, outdated.
                updates=[
                    InstanceUpdateEvent(
                        instance_id=instance_ids[2],
                        new_instance_status=Instance.REQUESTED,
                        instance_type="type-2",
                    ),
                ],
            )
        )
        assert reply.status.code == StatusCode.VERSION_MISMATCH
        assert len(subscriber.events) == 0

    def test_insert(self):
        ins_storage = InstanceStorage(
            "cluster-id",
            InMemoryStorage(),
        )
        subscriber = MockSubscriber()
        im = InstanceManager(
            ins_storage, instance_status_update_subscribers=[subscriber]
        )

        im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,
                updates=[
                    InstanceUpdateEvent(
                        instance_type="type-1",
                        instance_id="id-1",
                        new_instance_status=Instance.QUEUED,
                        upsert=True,
                    ),
                    InstanceUpdateEvent(
                        instance_id="id-2",
                        new_instance_status=Instance.TERMINATING,
                        cloud_instance_id="cloud-id-2",
                        upsert=True,
                    ),
                    InstanceUpdateEvent(
                        instance_id="id-3",
                        new_instance_status=Instance.ALLOCATED,
                        cloud_instance_id="cloud-id-3",
                        node_kind=NodeKind.WORKER,
                        instance_type="type-3",
                        upsert=True,
                    ),
                ],
            )
        )
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert len(reply.state.instances) == 3
        instance_by_ids = {ins.instance_id: ins for ins in reply.state.instances}
        assert instance_by_ids["id-1"].status == Instance.QUEUED
        assert instance_by_ids["id-1"].instance_type == "type-1"
        assert instance_by_ids["id-2"].status == Instance.TERMINATING
        assert instance_by_ids["id-3"].status == Instance.ALLOCATED
        assert instance_by_ids["id-3"].cloud_instance_id == "cloud-id-3"
        version = reply.state.version

        # With non-upsert flags.
        with pytest.raises(AssertionError):
            reply = im.update_instance_manager_state(
                UpdateInstanceManagerStateRequest(
                    expected_version=version,
                    updates=[
                        InstanceUpdateEvent(
                            instance_type="type-1",
                            instance_id="id-999",
                            new_instance_status=Instance.QUEUED,
                        ),
                    ],
                )
            )

        # With invalid statuses
        all_statuses = set(Instance.InstanceStatus.values())
        non_insertable_statuses = all_statuses - {
            Instance.QUEUED,
            Instance.TERMINATING,
            Instance.ALLOCATED,
        }

        for status in non_insertable_statuses:
            subscriber.clear()
            with pytest.raises(AssertionError):
                reply = im.update_instance_manager_state(
                    UpdateInstanceManagerStateRequest(
                        expected_version=version,
                        updates=[
                            InstanceUpdateEvent(
                                instance_id="id-999",
                                new_instance_status=status,
                            ),
                        ],
                    )
                )
            assert len(subscriber.events) == 0

    def test_apply_update(self):
        ins_storage = InstanceStorage(
            "cluster-id",
            InMemoryStorage(),
        )
        subscriber = MockSubscriber()
        im = InstanceManager(
            ins_storage, instance_status_update_subscribers=[subscriber]
        )

        # Insert a new instance.
        im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=0,
                updates=[
                    InstanceUpdateEvent(
                        instance_type="type-1",
                        instance_id="id-1",
                        new_instance_status=Instance.QUEUED,
                        upsert=True,
                    ),
                ],
            )
        )
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert len(reply.state.instances) == 1
        assert reply.state.instances[0].status == Instance.QUEUED
        assert reply.state.instances[0].instance_type == "type-1"

        # Request
        im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=1,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="id-1",
                        new_instance_status=Instance.REQUESTED,
                        launch_request_id="l1",
                        instance_type="type-1",
                    ),
                ],
            )
        )
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert len(reply.state.instances) == 1
        assert reply.state.instances[0].status == Instance.REQUESTED
        assert reply.state.instances[0].launch_request_id == "l1"

        # ALLOCATED
        im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=2,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="id-1",
                        new_instance_status=Instance.ALLOCATED,
                        cloud_instance_id="cloud-id-1",
                        node_kind=NodeKind.WORKER,
                        instance_type="type-1",
                    ),
                ],
            )
        )
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert len(reply.state.instances) == 1
        assert reply.state.instances[0].status == Instance.ALLOCATED
        assert reply.state.instances[0].cloud_instance_id == "cloud-id-1"

        # RAY_RUNNING
        im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=3,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="id-1",
                        new_instance_status=Instance.RAY_RUNNING,
                        ray_node_id="ray-node-1",
                    ),
                ],
            )
        )
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert len(reply.state.instances) == 1
        assert reply.state.instances[0].status == Instance.RAY_RUNNING
        assert reply.state.instances[0].node_id == "ray-node-1"

        # TERMINATED
        im.update_instance_manager_state(
            UpdateInstanceManagerStateRequest(
                expected_version=4,
                updates=[
                    InstanceUpdateEvent(
                        instance_id="id-1",
                        new_instance_status=Instance.TERMINATED,
                    ),
                ],
            )
        )
        reply = im.get_instance_manager_state(GetInstanceManagerStateRequest())
        assert len(reply.state.instances) == 1
        assert reply.state.instances[0].status == Instance.TERMINATED


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
