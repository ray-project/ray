# coding: utf-8
import copy
import os
import sys

import pytest  # noqa

from ray.autoscaler.v2.instance_manager.instance_storage import (
    InstanceStorage,
    InstanceUpdatedSuscriber,
    InstanceUpdateEvent,
)
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.core.generated.instance_manager_pb2 import Instance


class DummySubscriber(InstanceUpdatedSuscriber):
    def __init__(self):
        self.events = []

    def notify(self, events):
        self.events.extend(events)


def create_instance(
    instance_id, status=Instance.INSTANCE_STATUS_UNSPECIFIED, version=0
):
    return Instance(instance_id=instance_id, status=status, version=version)


def test_upsert():
    subscriber = DummySubscriber()

    storage = InstanceStorage(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
        status_change_subscriber=subscriber,
    )
    instance1 = create_instance("instance1")
    instance2 = create_instance("instance2")
    instance3 = create_instance("instance3")

    assert (True, 1) == storage.batch_upsert_instances(
        [instance1, instance2],
        expected_storage_version=None,
    )

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]

    instance1.version = 1
    instance2.version = 1
    entries, storage_version = storage.get_instances()

    assert storage_version == 1
    assert entries == {
        "instance1": instance1,
        "instance2": instance2,
    }

    assert (False, 1) == storage.batch_upsert_instances(
        [create_instance("instance1"), create_instance("instance2")],
        expected_storage_version=0,
    )

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]

    instance2.status = Instance.IDLE
    assert (True, 2) == storage.batch_upsert_instances(
        [instance3, instance2],
        expected_storage_version=1,
    )

    instance1.version = 1
    instance2.version = 2
    instance3.version = 2
    entries, storage_version = storage.get_instances()

    assert storage_version == 2
    assert entries == {
        "instance1": instance1,
        "instance2": instance2,
        "instance3": instance3,
    }

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance3", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.IDLE),
    ]


def test_update():
    subscriber = DummySubscriber()

    storage = InstanceStorage(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
        status_change_subscriber=subscriber,
    )
    instance1 = create_instance("instance1")
    instance2 = create_instance("instance2")

    assert (True, 1) == storage.upsert_instance(instance=instance1)
    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]
    assert (True, 2) == storage.upsert_instance(instance=instance2)

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]

    assert (
        {
            "instance1": create_instance("instance1", version=1),
            "instance2": create_instance("instance2", version=2),
        },
        2,
    ) == storage.get_instances()

    # failed because instance version is not correct
    assert (False, 2) == storage.upsert_instance(
        instance=instance1,
        expected_instance_version=0,
    )

    # failed because storage version is not correct
    assert (False, 2) == storage.upsert_instance(
        instance=instance1,
        expected_storage_verison=0,
    )

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]

    assert (True, 3) == storage.upsert_instance(
        instance=instance2,
        expected_storage_verison=2,
    )

    assert (
        {
            "instance1": create_instance("instance1", version=1),
            "instance2": create_instance("instance2", version=3),
        },
        3,
    ) == storage.get_instances()

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]

    assert (True, 4) == storage.upsert_instance(
        instance=instance1,
        expected_instance_version=1,
    )

    assert (
        {
            "instance1": create_instance("instance1", version=4),
            "instance2": create_instance("instance2", version=3),
        },
        4,
    ) == storage.get_instances()

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
    ]


def test_delete():
    subscriber = DummySubscriber()

    storage = InstanceStorage(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
        status_change_subscriber=subscriber,
    )
    instance1 = create_instance("instance1")
    instance2 = create_instance("instance2")
    instance3 = create_instance("instance3")

    assert (True, 1) == storage.batch_upsert_instances(
        [instance1, instance2, instance3],
        expected_storage_version=None,
    )

    assert (False, 1) == storage.batch_delete_instances(
        instance_ids=["instance1"], expected_storage_version=0
    )
    assert (True, 2) == storage.batch_delete_instances(instance_ids=["instance1"])

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance3", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance1", Instance.GARAGE_COLLECTED),
    ]

    assert (
        {
            "instance2": create_instance("instance2", version=1),
            "instance3": create_instance("instance3", version=1),
        },
        2,
    ) == storage.get_instances()

    assert (True, 3) == storage.batch_delete_instances(
        instance_ids=["instance2"], expected_storage_version=2
    )

    assert (
        {
            "instance3": create_instance("instance3", version=1),
        },
        3,
    ) == storage.get_instances()

    assert subscriber.events == [
        InstanceUpdateEvent("instance1", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance2", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance3", Instance.INSTANCE_STATUS_UNSPECIFIED),
        InstanceUpdateEvent("instance1", Instance.GARAGE_COLLECTED),
        InstanceUpdateEvent("instance2", Instance.GARAGE_COLLECTED),
    ]


def test_get_instances():
    storage = InstanceStorage(
        cluster_id="test_cluster",
        storage=InMemoryStorage(),
    )
    instance1 = create_instance("instance1", version=1)
    instance2 = create_instance("instance2", status=Instance.RUNNING, version=1)
    instance3 = create_instance("instance3", status=Instance.IDLE, version=1)

    assert (True, 1) == storage.batch_upsert_instances(
        [copy.deepcopy(instance1), copy.deepcopy(instance2), copy.deepcopy(instance3)],
        expected_storage_version=None,
    )

    assert (
        {
            "instance1": instance1,
            "instance2": instance2,
            "instance3": instance3,
        },
        1,
    ) == storage.get_instances()

    assert (
        {
            "instance1": instance1,
            "instance2": instance2,
        },
        1,
    ) == storage.get_instances(instance_ids=["instance1", "instance2"])

    assert ({"instance2": instance2}, 1) == storage.get_instances(
        instance_ids=["instance1", "instance2"], status_filter={Instance.RUNNING}
    )

    assert (
        {
            "instance2": instance2,
        },
        1,
    ) == storage.get_instances(status_filter={Instance.RUNNING})


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
