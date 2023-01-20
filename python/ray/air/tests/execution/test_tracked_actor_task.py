from collections import Counter

import pytest

import ray
from ray.air import ResourceRequest
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal.event_manager import RayEventManager, EventType


RESOURCE_MANAGERS = [FixedResourceManager, PlacementGroupResourceManager]


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


@ray.remote
class Actor:
    def foo(self, val, error: bool = False):
        if error:
            raise RuntimeError
        return val


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
def test_resolve(ray_start_4_cpus, resource_manager_cls):
    """Test that the `on_result` callback is invoked when a task completes.

    - Instantiate global data object
    - Schedule task that returns a value
    - The callback writes the returned value to the global data object
    """
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    seen = {"data": 0}

    def result_callback(tracked_actor, result):
        seen["data"] = result

    tracked_actor = event_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    event_manager.schedule_actor_task(tracked_actor, "foo", (4, False)).on_result(
        result_callback
    )
    event_manager.wait(timeout=5, event_type=EventType.ACTORS)
    event_manager.wait(event_type=EventType.TASKS)

    assert seen["data"] == 4


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
@pytest.mark.parametrize("num_tasks", [1, 10, 100])
@pytest.mark.parametrize("wait_for_events", [None, 1, 10])
def test_resolve_many(
    ray_start_4_cpus, resource_manager_cls, num_tasks, wait_for_events
):
    """Schedule ``num_tasks`` tasks and wait until ``wait_for_events`` of them resolve.

    Every resolved task will increase a counter by its return value (1).

    If wait_for_events = None, we expect num_tasks tasks to resolve.
    If wait_for_events is a number, we expect that many tasks to resolve.
    """
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    seen = {"data": 0}

    def result_callback(tracked_actor, result):
        seen["data"] += result

    tracked_actor = event_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    event_manager.wait(timeout=5, event_type=EventType.ACTORS)

    for i in range(num_tasks):
        event_manager.schedule_actor_task(tracked_actor, "foo", (1, False)).on_result(
            result_callback
        )

    if wait_for_events and wait_for_events > num_tasks:
        expected_num_events = 0
        with pytest.raises(ValueError):
            event_manager.wait(num_events=wait_for_events, event_type=EventType.TASKS)

    else:
        expected_num_events = wait_for_events or num_tasks
        event_manager.wait(num_events=wait_for_events, event_type=EventType.TASKS)

    assert seen["data"] == expected_num_events


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
def test_error_noop(ray_start_4_cpus, resource_manager_cls):
    """When no `on_error` callback is specified, errors should be ignored."""
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    tracked_actor = event_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    event_manager.schedule_actor_task(tracked_actor, "foo", (1, True))
    event_manager.wait(timeout=5, event_type=EventType.ACTORS)
    event_manager.wait(event_type=EventType.TASKS)


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
def test_error_custom(ray_start_4_cpus, resource_manager_cls):
    """When an `on_error` callback is specified, it is invoked."""
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    stats = Counter()

    def error_callback(tracked_actor, exception):
        stats["exception"] += 1

    tracked_actor = event_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    event_manager.schedule_actor_task(tracked_actor, "foo", (1, True)).on_error(
        error_callback
    )

    event_manager.wait(timeout=10, event_type=EventType.ACTORS)

    event_manager.wait(event_type=EventType.TASKS)
    assert stats["exception"] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
