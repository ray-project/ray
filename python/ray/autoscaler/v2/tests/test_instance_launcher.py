# coding: utf-8
import os
import sys
from typing import Dict

import pytest  # noqa

from mock import MagicMock

from ray._private.test_utils import wait_for_condition
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceUtil
from ray.autoscaler.v2.instance_manager.instance_storage import InstanceStorage
from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage
from ray.autoscaler.v2.instance_manager.subscribers.instance_launcher import (
    InstanceLauncher,
    logger,
)
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.instance_manager_pb2 import Instance, InstanceUpdateEvent

logger.setLevel("DEBUG")


def add_instances(storage, instances):
    success, version = storage.batch_upsert_instances(instances)
    assert success
    for instance in instances:
        instance.version = version
    return instances


def notify(launcher, instance_id, new_status):
    event = InstanceUpdateEvent(
        instance_id=instance_id,
        new_instance_status=new_status,
    )
    launcher.notify([event])


def get_launch_requests(node_provider_mock) -> Dict[str, Dict[str, int]]:
    return {
        call.kwargs["request_id"]: call.kwargs["shape"]
        for call in node_provider_mock.launch.call_args_list
    }


def update_instance(storage, instance, new_status) -> Instance:
    InstanceUtil.set_status(instance, new_status)
    success, version = storage.batch_upsert_instances([instance])
    assert success
    instance.version = version
    return instance


def test_launch_basic():
    instance_storage = InstanceStorage(cluster_id="test", storage=InMemoryStorage())
    node_provider = MagicMock()
    launcher = InstanceLauncher(
        instance_storage=instance_storage,
        node_provider=node_provider,
        upscaling_speed=1,
    )

    instance = create_instance("1", status=Instance.QUEUED, instance_type="type-1")
    instance2 = create_instance("2", status=Instance.QUEUED, instance_type="type-2")
    add_instances(instance_storage, [instance, instance2])
    notify(launcher, "1", Instance.QUEUED)

    def verify():
        launch_requests = get_launch_requests(node_provider)
        assert len(launch_requests) == 1
        assert list(launch_requests.values())[0] == {"type-1": 1, "type-2": 1}
        instance = instance_storage.get_instances()[0]["1"]
        assert launch_requests.keys() == {instance.launch_request_id}

        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "upscaling_speed,max_concurrent_launches", [(0, 10), (1, 10), (10, 10), (100, 10)]
)
def test_launch_upscaling_limited(upscaling_speed, max_concurrent_launches):
    instance_storage = InstanceStorage(cluster_id="test", storage=InMemoryStorage())
    node_provider = MagicMock()
    launcher = InstanceLauncher(
        instance_storage=instance_storage,
        node_provider=node_provider,
        upscaling_speed=upscaling_speed,
        max_concurrent_launches=max_concurrent_launches,
    )

    # Start with a 2 instance cluster
    instance1 = create_instance(
        "999", status=Instance.ALLOCATED, instance_type="type-1"
    )
    instance2 = create_instance(
        "1000", status=Instance.REQUESTED, instance_type="type-1"
    )
    add_instances(instance_storage, [instance1, instance2])

    expected_launch_num = min(max(upscaling_speed * 2, 1), max_concurrent_launches - 1)

    queued_instances = [
        create_instance(str(i), status=Instance.QUEUED, instance_type="type-1")
        for i in range(100)
    ]
    add_instances(instance_storage, queued_instances)
    notify(launcher, "1", Instance.QUEUED)

    def verify():
        launch_requests = get_launch_requests(node_provider)
        assert len(launch_requests) == 1 if expected_launch_num > 0 else 0
        if expected_launch_num > 0:
            assert list(launch_requests.values())[0] == {"type-1": expected_launch_num}

        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "max_concurrent_launches,num_allocated,num_requested",
    [(1, 0, 0), (10, 0, 0), (1, 0, 1), (1, 1, 0), (10, 1, 0), (10, 0, 1), (10, 5, 5)],
)
def test_max_concurrent_launches(
    max_concurrent_launches,
    num_allocated,
    num_requested,
):
    instance_storage = InstanceStorage(cluster_id="test", storage=InMemoryStorage())
    node_provider = MagicMock()
    upscaling_speed = 100000  # large values to maximize per type upscaling
    launcher = InstanceLauncher(
        instance_storage=instance_storage,
        node_provider=node_provider,
        upscaling_speed=upscaling_speed,
        max_concurrent_launches=max_concurrent_launches,
    )
    next_id = 0

    # Add some allocated instances.
    for _ in range(num_allocated):
        instance = create_instance(
            str(next_id), status=Instance.ALLOCATED, instance_type="type-1"
        )
        add_instances(instance_storage, [instance])
        next_id += 1

    # Add some requested instances.
    for _ in range(num_requested):
        instance = create_instance(
            str(next_id), status=Instance.REQUESTED, instance_type="type-1"
        )
        add_instances(instance_storage, [instance])
        next_id += 1

    # Add many queued instances.
    queued_instances = [
        create_instance(
            str(i + next_id), status=Instance.QUEUED, instance_type="type-1"
        )
        for i in range(1000)
    ]
    add_instances(instance_storage, queued_instances)

    # Trigger the launch.
    notify(launcher, str(next_id), Instance.QUEUED)

    num_desired_upscale = max(1, upscaling_speed * (num_requested + num_allocated))
    expected_launch_num = min(
        num_desired_upscale,
        max(0, max_concurrent_launches - num_requested),  # global limit
    )

    def verify():
        launch_requests = get_launch_requests(node_provider)
        assert len(launch_requests) == (1 if expected_launch_num > 0 else 0)
        if expected_launch_num > 0:
            assert list(launch_requests.values())[0] == {"type-1": expected_launch_num}

        return True

    wait_for_condition(verify)


def test_launch_same_request_id():
    instance_storage = InstanceStorage(cluster_id="test", storage=InMemoryStorage())
    node_provider = MagicMock()
    launcher = InstanceLauncher(
        instance_storage=instance_storage,
        node_provider=node_provider,
        upscaling_speed=1,
    )

    instance = create_instance("1", status=Instance.QUEUED, instance_type="type-1")
    add_instances(instance_storage, [instance])
    notify(launcher, "1", Instance.QUEUED)

    def verify():
        launch_requests = get_launch_requests(node_provider)
        assert len(launch_requests) == 1
        assert list(launch_requests.values())[0] == {"type-1": 1}
        instance = instance_storage.get_instances()[0]["1"]
        assert launch_requests.keys() == {instance.launch_request_id}

        return True

    wait_for_condition(verify)

    # Add a new instance to be queued.
    instance2 = create_instance("2", status=Instance.QUEUED, instance_type="type-1")
    add_instances(instance_storage, [instance2])
    # Update the instance to be queued again.
    instance1 = instance_storage.get_instances()[0]["1"]
    update_instance(instance_storage, instance1, Instance.ALLOCATION_FAILED)
    update_instance(instance_storage, instance1, Instance.QUEUED)

    notify(launcher, "1", Instance.QUEUED)

    def verify():
        # We should launch with the same request id for "1", but new request id
        # for "2".
        launch_requests = get_launch_requests(node_provider)
        assert len(launch_requests) == 2
        instance = instance_storage.get_instances()[0]["1"]
        assert launch_requests[instance.launch_request_id] == {"type-1": 1}
        instance = instance_storage.get_instances()[0]["2"]
        assert launch_requests[instance.launch_request_id] == {"type-1": 1}

        return True

    wait_for_condition(verify)


def test_launch_failed():
    """
    Launch failure when:
        1. version changed during the update.
    """
    node_provider = MagicMock()
    instance_storage = MagicMock()

    check = {"success": False, "result": None}

    def launch_callback_failed(fut):
        result = fut.result()
        check["success"] = result is None
        check["result"] = result

    launcher = InstanceLauncher(
        instance_storage=instance_storage,
        node_provider=node_provider,
        upscaling_speed=1,
        launch_callback=launch_callback_failed,
    )

    instance = create_instance("1", status=Instance.QUEUED, instance_type="type-1")

    instance_storage.batch_upsert_instances.return_value = True, 1
    add_instances(instance_storage, [instance])

    # TEST 1: instance update fail with version mismatch
    instance_storage.get_instances.return_value = {
        "1": instance,
    }
    # instance update fail with version mismatch
    instance_storage.batch_upsert_instances.return_value = False, 2
    notify(launcher, "1", Instance.QUEUED)

    def verify():
        assert check["success"] is False, check["result"]
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
