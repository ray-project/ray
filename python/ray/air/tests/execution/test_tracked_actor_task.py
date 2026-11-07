from collections import Counter

import pytest

import ray
from ray.air import ResourceRequest
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal.actor_manager import RayActorManager

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
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    seen = {"data": 0}

    def result_callback(tracked_actor, result):
        seen["data"] = result

    tracked_actor = actor_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    actor_manager.schedule_actor_task(
        tracked_actor, "foo", (4, False), on_result=result_callback
    )
    actor_manager.next()
    actor_manager.next()

    assert seen["data"] == 4


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
@pytest.mark.parametrize("num_tasks", [1, 10, 100])
def test_resolve_many(ray_start_4_cpus, resource_manager_cls, num_tasks):
    """Schedule ``num_tasks`` tasks and wait until ``wait_for_events`` of them resolve.

    Every resolved task will increase a counter by its return value (1).
    """
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    seen = {"data": 0}

    def result_callback(tracked_actor, result):
        seen["data"] += result

    tracked_actor = actor_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    actor_manager.next()

    for i in range(num_tasks):
        actor_manager.schedule_actor_task(
            tracked_actor, "foo", (1, False), on_result=result_callback
        )

    for i in range(num_tasks):
        actor_manager.next()
        assert seen["data"] == i + 1


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
def test_error_noop(ray_start_4_cpus, resource_manager_cls):
    """When no `on_error` callback is specified, errors should be ignored."""
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    tracked_actor = actor_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    actor_manager.schedule_actor_task(tracked_actor, "foo", (1, True))
    actor_manager.next()
    actor_manager.next()


@pytest.mark.parametrize("resource_manager_cls", RESOURCE_MANAGERS)
def test_error_custom(ray_start_4_cpus, resource_manager_cls):
    """When an `on_error` callback is specified, it is invoked."""
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    stats = Counter()

    def error_callback(tracked_actor, exception):
        stats["exception"] += 1

    tracked_actor = actor_manager.add_actor(
        cls=Actor, kwargs={}, resource_request=ResourceRequest([{"CPU": 4}])
    )
    actor_manager.schedule_actor_task(
        tracked_actor, "foo", (1, True), on_error=error_callback
    )

    actor_manager.next()
    actor_manager.next()
    assert stats["exception"] == 1


def test_pending_actors_fifo_deque_and_schedule_actor_tasks_validation():
    """Verify pending actors use a FIFO deque and schedule_actor_tasks raises ValueError on length mismatch."""
    from collections import deque
    from unittest.mock import MagicMock

    resource_manager = MagicMock()
    resource_manager.has_resources_ready.return_value = False
    actor_manager = RayActorManager(resource_manager=resource_manager)

    req = ResourceRequest([{"CPU": 1}])
    a1 = actor_manager.add_actor(cls=Actor, kwargs={}, resource_request=req)
    a2 = actor_manager.add_actor(cls=Actor, kwargs={}, resource_request=req)
    a3 = actor_manager.add_actor(cls=Actor, kwargs={}, resource_request=req)

    pending = actor_manager._resource_request_to_pending_actors[req]
    assert isinstance(pending, deque)
    assert list(pending) == [a1, a2, a3]

    # Removing a middle pending actor preserves FIFO order of remaining actors
    assert actor_manager.remove_actor(a2) is False
    assert list(pending) == [a1, a3]

    # Mismatched args list with kwargs=None raises ValueError (not TypeError from len(None))
    with pytest.raises(ValueError, match=r"len\(args\)=1"):
        actor_manager.schedule_actor_tasks([a1, a3], "foo", args=[(1,)])

    # Mismatched kwargs list with args=None raises ValueError (not TypeError from len(None))
    with pytest.raises(ValueError, match=r"len\(kwargs\)=1"):
        actor_manager.schedule_actor_tasks([a1, a3], "foo", kwargs=[{"val": 1}])



if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
