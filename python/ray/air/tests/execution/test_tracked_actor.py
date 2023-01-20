from collections import Counter
from typing import Any, Optional, Type

import pytest

import ray
from ray.air import ResourceRequest
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal.event_manager import RayEventManager, EventType


def _raise(exception_type: Type[Exception] = RuntimeError, msg: Optional[str] = None):
    def _raise_exception(*args, **kwargs):
        raise exception_type(msg)

    return _raise_exception


class Started(RuntimeError):
    pass


class Stopped(RuntimeError):
    pass


class Failed(RuntimeError):
    pass


class Result(RuntimeError):
    pass


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


class Actor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get_kwargs(self):
        return self.kwargs

    def task(self, value: Any):
        return value


@ray.remote(num_cpus=4)
def fn():
    return True


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("actor_cls", [Actor, ray.remote(Actor)])
@pytest.mark.parametrize("kill", [False, True])
def test_start_stop_actor(ray_start_4_cpus, resource_manager_cls, actor_cls, kill):
    """Test that starting and stopping actors work and invokes a callback.

    - Start an actor
    - Starting should trigger start callback
    - Schedule actor task, which should resolve (meaning actor successfully started)
    - Stop actor, which should resolve and trigger stop callback
    - Schedule remote fn that takes up all cluster resources. This should resolve,
      meaning that the actor was stopped successfully.
    """
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    # Start actor, set callbacks
    tracked_actor = (
        event_manager.add_actor(
            cls=actor_cls,
            kwargs={"key": "val"},
            resource_request=ResourceRequest([{"CPU": 4}]),
        )
        .on_start(_raise(Started))
        .on_stop(_raise(Stopped))
        .on_error(_raise(Failed))
    )

    # Actor should be started
    with pytest.raises(Started):
        event_manager.wait(num_events=1, timeout=5)

    # Schedule task on actor which should resolve (actor successfully started)
    event_manager.schedule_actor_task(tracked_actor, "task", (1,)).on_result(
        _raise(Result)
    )

    with pytest.raises(Result):
        event_manager.wait(num_events=1, event_type=EventType.TASKS)

    # Now we can assert that there are no CPUS resources available anymore.
    # Note that actor starting is asynchronous, so we can't assert this right away
    # - that's why we wait for the actor task to resolve first.
    assert ray.available_resources().get("CPU", 0.0) == 0, ray.available_resources()

    # Stop actor
    event_manager.remove_actor(tracked_actor, kill=kill)

    with pytest.raises(Stopped):
        event_manager.wait(num_events=1, timeout=5)

    # This task takes up all the cluster resources. It should resolve now that
    # the actor was terminated.
    assert ray.get(fn.remote(), timeout=5)


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_start_many_actors(ray_start_4_cpus, resource_manager_cls):
    """Test that starting more actors than fit onto the cluster works.

    - Request 10 actors
    - 4 can be started. Assert they are started
    - Stop 2
    - Assert 2 are stopped and 2 new ones are started
    """
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    running_actors = []
    # stats keeps track of started/stopped actors
    stats = Counter()

    def start_callback(tracked_actor):
        running_actors.append(tracked_actor)
        stats["started"] += 1

    def stop_callback(tracked_actor):
        running_actors.remove(tracked_actor)
        stats["stopped"] += 1

    # start 10 actors
    for i in range(10):
        event_manager.add_actor(
            cls=Actor,
            kwargs={"key": "val"},
            resource_request=ResourceRequest([{"CPU": 1}]),
        ).on_start(start_callback).on_stop(stop_callback).on_error(_raise(Failed))

    # wait for some actor starts
    event_manager.wait(timeout=5)

    # we should now have 4 started actors
    assert stats["started"] == 4
    assert stats["stopped"] == 0
    assert len(running_actors) == 4

    # stop 2 actors
    event_manager.remove_actor(running_actors[0])
    event_manager.remove_actor(running_actors[1])

    # Wait two times, once for termination, once for start
    event_manager.wait(num_events=2, timeout=5)
    event_manager.wait(num_events=2, timeout=5)

    # we should have 4 running actors, 6 started and 2 stopped
    assert stats["started"] == 6
    assert stats["stopped"] == 2
    assert len(running_actors) == 4


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("where", ["init", "fn"])
def test_actor_fail(ray_start_4_cpus, resource_manager_cls, where):
    """Test that actor failures are handled properly.

    - Start actor that either fails on init or in a task (RayActorError)
    - Schedule task on actor
    - Assert that the correct callbacks are called
    """
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    # keep track of failed tasks and actors
    stats = Counter()

    @ray.remote
    class FailingActor:
        def __init__(self, where):
            self._where = where
            if self._where == "init":
                raise RuntimeError("INIT")

        def fn(self):
            if self._where == "fn":
                # SystemExit will invoke a RayActorError
                raise SystemExit
            return True

    def fail_callback_actor(tracked_actor, exception):
        stats["failed_actor"] += 1

    def fail_callback_task(tracked_actor, exception):
        stats["failed_task"] += 1

    # Start actor
    tracked_actor = event_manager.add_actor(
        cls=FailingActor,
        kwargs={"where": where},
        resource_request=ResourceRequest([{"CPU": 1}]),
    ).on_error(fail_callback_actor)

    # Wait until it is started. This won't invoke any callback, yet
    event_manager.wait(timeout=5)

    assert stats["failed_actor"] == 0
    assert stats["failed_task"] == 0

    # Schedule task
    event_manager.schedule_actor_task(tracked_actor, "fn").on_error(fail_callback_task)

    # Yield control and wait for task resolution. This will invoke the callback.
    event_manager.wait(timeout=5)

    assert stats["failed_actor"] == 1
    assert stats["failed_task"] == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
