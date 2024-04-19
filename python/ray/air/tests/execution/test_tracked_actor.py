import gc
import threading
import time
from collections import Counter
from typing import Any, Optional, Type

import pytest

import ray
from ray.air import ResourceRequest
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal import Barrier
from ray.air.execution._internal.actor_manager import RayActorManager


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


@pytest.fixture
def cleanup():
    # Garbage collect at the start
    # This ensures that all resources are freed up for the upcoming test.
    gc.collect()
    yield


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
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    # Start actor, set callbacks
    tracked_actor = actor_manager.add_actor(
        cls=actor_cls,
        kwargs={"key": "val"},
        resource_request=ResourceRequest([{"CPU": 4}]),
        on_start=_raise(Started),
        on_stop=_raise(Stopped),
        on_error=_raise(Failed),
    )

    # Actor should be started
    with pytest.raises(Started):
        actor_manager.next()

    # Schedule task on actor which should resolve (actor successfully started)
    actor_manager.schedule_actor_task(
        tracked_actor, "task", (1,), on_result=_raise(Result)
    )

    with pytest.raises(Result):
        actor_manager.next()

    # Now we can assert that there are no CPUS resources available anymore.
    # Note that actor starting is asynchronous, so we can't assert this right away
    # - that's why we wait for the actor task to resolve first.
    assert ray.available_resources().get("CPU", 0.0) == 0, ray.available_resources()

    # Stop actor
    actor_manager.remove_actor(tracked_actor, kill=kill)

    with pytest.raises(Stopped):
        actor_manager.next()

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
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

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
    expected_actors = []
    for i in range(10):
        tracked_actor = actor_manager.add_actor(
            cls=Actor,
            kwargs={"key": "val"},
            resource_request=ResourceRequest([{"CPU": 1}]),
            on_start=start_callback,
            on_stop=stop_callback,
            on_error=_raise(Failed),
        )
        expected_actors.append(tracked_actor)

    # wait for some actor starts
    for i in range(4):
        actor_manager.next()

    # we should now have 4 started actors
    assert stats["started"] == 4
    assert stats["stopped"] == 0
    assert len(running_actors) == 4
    assert set(running_actors) == set(expected_actors[:4])

    # stop 2 actors
    actor_manager.remove_actor(running_actors[0])
    actor_manager.remove_actor(running_actors[1])

    # Wait four times, twice for termination, twice for start
    for i in range(4):
        actor_manager.next()

    # we should have 4 running actors, 6 started and 2 stopped
    assert stats["started"] == 6
    assert stats["stopped"] == 2
    assert len(running_actors) == 4


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("where", ["init", "fn"])
def test_actor_fail(ray_start_4_cpus, cleanup, resource_manager_cls, where):
    """Test that actor failures are handled properly.

    - Start actor that either fails on init or in a task (RayActorError)
    - Schedule task on actor
    - Assert that the correct callbacks are called
    """
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

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
    tracked_actor = actor_manager.add_actor(
        cls=FailingActor,
        kwargs={"where": where},
        resource_request=ResourceRequest([{"CPU": 1}]),
        on_error=fail_callback_actor,
    )

    if where != "init":
        # Wait until it is started. This won't invoke any callback, yet
        actor_manager.next()

        assert stats["failed_actor"] == 0
        assert stats["failed_task"] == 0

        # Schedule task
        actor_manager.schedule_actor_task(
            tracked_actor, "fn", on_error=fail_callback_task
        )

    # Yield control and wait for task resolution. This will invoke the callback.
    actor_manager.next()

    assert stats["failed_actor"] == 1
    assert stats["failed_task"] == bool(where != "init")


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_stop_actor_before_start(
    ray_start_4_cpus, tmp_path, cleanup, resource_manager_cls
):
    """Test that actor failures are handled properly.

    - Start actor that either fails on init or in a task (RayActorError)
    - Schedule task on actor
    - Assert that the correct callbacks are called
    """
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    hang_marker = tmp_path / "hang.txt"

    @ray.remote
    class HangingActor:
        def __init__(self):
            while not hang_marker.exists():
                time.sleep(0.05)

    tracked_actor = actor_manager.add_actor(
        HangingActor,
        kwargs={},
        resource_request=ResourceRequest([{"CPU": 1}]),
        on_start=_raise(RuntimeError, "Should not have started"),
        on_stop=_raise(RuntimeError, "Should not have stopped"),
    )
    while not actor_manager.is_actor_started(tracked_actor):
        actor_manager.next(0.05)

    # Actor started but hasn't triggered on_start, yet
    actor_manager.remove_actor(tracked_actor)
    hang_marker.write_text("")
    while actor_manager.is_actor_started(tracked_actor):
        actor_manager.next(0.05)

    assert actor_manager.num_live_actors == 0


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize("start_thread", [False, True])
def test_stop_actor_custom_future(
    ray_start_4_cpus, tmp_path, cleanup, resource_manager_cls, start_thread
):
    """If we pass a custom stop future, the actor should still be shutdown by GC.

    This should also be the case when we start a thread in the background, as we
    do e.g. in Ray Tune's function runner.
    """
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    hang_marker = tmp_path / "hang.txt"

    actor_name = f"stopping_actor_{resource_manager_cls.__name__}_{start_thread}"

    @ray.remote(name=actor_name)
    class HangingStopActor:
        def __init__(self):
            self._thread = None
            self._stop_event = threading.Event()
            if start_thread:

                def entrypoint():
                    while True:
                        print("Thread!")
                        time.sleep(1)
                        if self._stop_event.is_set():
                            sys.exit(0)

                self._thread = threading.Thread(target=entrypoint)
                self._thread.start()

        def stop(self):
            print("Waiting")
            while not hang_marker.exists():
                time.sleep(0.05)
            self._stop_event.set()
            print("stopped")

    start_barrier = Barrier(max_results=1)
    stop_barrier = Barrier(max_results=1)

    tracked_actor = actor_manager.add_actor(
        HangingStopActor,
        kwargs={},
        resource_request=ResourceRequest([{"CPU": 1}]),
        on_start=start_barrier.arrive,
        on_stop=stop_barrier.arrive,
    )
    while not start_barrier.completed:
        actor_manager.next(0.05)

    # Actor is alive
    assert ray.get_actor(actor_name)

    stop_future = actor_manager.schedule_actor_task(tracked_actor, "stop")
    actor_manager.remove_actor(tracked_actor, kill=False, stop_future=stop_future)

    assert not stop_barrier.completed

    hang_marker.write_text("!")

    while not stop_barrier.completed:
        actor_manager.next(0.05)

    # Actor should have stopped now and should get cleaned up
    with pytest.raises(ValueError):
        ray.get_actor(actor_name)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
