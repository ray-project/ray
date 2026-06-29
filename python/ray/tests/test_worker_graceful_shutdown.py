import os
import signal
import sys
from typing import List

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import wait_for_pid_to_exit

# Concurrency models that allow already-running tasks to keep executing after a
# graceful exit is requested. Single-threaded actors are excluded: they run
# tasks one at a time, so there is never a concurrently in-flight task to drain.
CONCURRENT_ACTOR_TYPES = ["asyncio", "threaded"]


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows doesn't handle SIGTERM gracefully."
)
@pytest.mark.parametrize("actor_type", CONCURRENT_ACTOR_TYPES)
def test_ray_get_during_graceful_shutdown(ray_start_regular_shared, actor_type: str):
    """Test that ray.get works as expected when draining tasks during shutdown.

    This only applies to concurrent actors (threaded and asyncio); single-threaded
    actors do not allow tasks to finish executing after SIGTERM.
    """
    signal_actor = SignalActor.remote()

    if actor_type == "asyncio":

        @ray.remote
        class A:
            def exit(self):
                os.kill(os.getpid(), signal.SIGTERM)

            async def wait_then_get(self, nested_ref: List[ray.ObjectRef]) -> str:
                print("Waiting for signal...")
                await signal_actor.wait.remote()
                print("Got signal, calling ray.get")
                return await nested_ref[0]

    elif actor_type == "threaded":

        @ray.remote(max_concurrency=2)
        class A:
            def exit(self):
                os.kill(os.getpid(), signal.SIGTERM)

            def wait_then_get(self, nested_ref: List[ray.ObjectRef]):
                print("Waiting for signal...")
                ray.get(signal_actor.wait.remote())
                print("Got signal, calling ray.get")
                return ray.get(nested_ref[0])

    # Start the actor and wait for the method to begin executing and then block.
    actor = A.remote()
    wait_ref = actor.wait_then_get.remote([ray.put("hi")])
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # SIGTERM the process and then signal the method to unblock.
    ray.get(actor.exit.remote())
    ray.get(signal_actor.send.remote())

    # Check that the method succeeds as expected.
    assert ray.get(wait_ref) == "hi"


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Graceful shutdown draining is unreliable on Windows.",
)
@pytest.mark.parametrize("actor_type", CONCURRENT_ACTOR_TYPES)
def test_exit_actor_delivers_inflight_task_results(
    ray_start_regular_shared, actor_type: str
):
    """In-flight tasks (already executing) on a concurrent actor finish and
    deliver their results when exit_actor() is called from another task,
    instead of failing with ActorDiedError. Covers both threaded actors (tasks
    on a thread pool) and asyncio actors (coroutines on the event loop)."""
    signal_actor = SignalActor.remote()
    num_tasks = 3

    if actor_type == "asyncio":

        @ray.remote
        class A:
            async def exit(self):
                ray.actor.exit_actor()

            async def wait_then_return(self, value):
                await signal_actor.wait.remote()
                return value

    else:

        @ray.remote(max_concurrency=num_tasks + 1)
        class A:
            def exit(self):
                ray.actor.exit_actor()

            def wait_then_return(self, value):
                ray.get(signal_actor.wait.remote())
                return value

    a = A.remote()
    refs = [a.wait_then_return.remote(i) for i in range(num_tasks)]
    # Wait until all tasks have started executing and are blocked on the signal.
    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == num_tasks
    )
    # Request the exit, then unblock the in-flight tasks.
    exit_ref = a.exit.remote()
    ray.get(signal_actor.send.remote())

    assert ray.get(refs, timeout=30) == list(range(num_tasks))
    # The task that called exit_actor() exits the actor, so its caller observes
    # the actor death.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(exit_ref, timeout=30)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Graceful shutdown draining is unreliable on Windows.",
)
@pytest.mark.parametrize("actor_type", CONCURRENT_ACTOR_TYPES)
def test_exit_actor_delivers_inflight_task_errors(
    ray_start_regular_shared, actor_type: str
):
    """An in-flight task on a concurrent actor that raises an application
    exception while the actor is gracefully exiting delivers that exception,
    not ActorDiedError."""
    signal_actor = SignalActor.remote()

    if actor_type == "asyncio":

        @ray.remote
        class A:
            async def exit(self):
                ray.actor.exit_actor()

            async def wait_then_raise(self):
                await signal_actor.wait.remote()
                raise ValueError("application error")

    else:

        @ray.remote(max_concurrency=2)
        class A:
            def exit(self):
                ray.actor.exit_actor()

            def wait_then_raise(self):
                ray.get(signal_actor.wait.remote())
                raise ValueError("application error")

    a = A.remote()
    ref = a.wait_then_raise.remote()
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)
    exit_ref = a.exit.remote()
    ray.get(signal_actor.send.remote())

    with pytest.raises(ValueError, match="application error"):
        ray.get(ref, timeout=30)
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(exit_ref, timeout=30)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Graceful shutdown draining is unreliable on Windows.",
)
@pytest.mark.parametrize("actor_type", CONCURRENT_ACTOR_TYPES)
def test_exit_actor_fails_queued_tasks(ray_start_regular_shared, actor_type: str):
    """Methods queued (submitted but not yet started executing) when
    exit_actor() is called fail with ActorDiedError, for both threaded and
    asyncio actors. ``max_concurrency`` bounds how many methods may run at once,
    so the ``work`` calls cannot start while the blocking methods hold every
    slot and are failed when the actor exits."""
    signal_actor = SignalActor.remote()
    max_concurrency = 2

    if actor_type == "asyncio":

        @ray.remote(max_concurrency=max_concurrency)
        class A:
            async def block_then_exit(self):
                # Hold the concurrency slot until released, then exit the actor.
                await signal_actor.wait.remote()
                ray.actor.exit_actor()

            async def work(self, value):
                return value

    else:

        @ray.remote(max_concurrency=max_concurrency)
        class A:
            def block_then_exit(self):
                # Hold the concurrency slot until released, then exit the actor.
                ray.get(signal_actor.wait.remote())
                ray.actor.exit_actor()

            def work(self, value):
                return value

    a = A.remote()
    # Fill every concurrency slot with a blocking task so later calls can't
    # start and remain queued.
    blocking_refs = [a.block_then_exit.remote() for _ in range(max_concurrency)]
    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == max_concurrency
    )
    # These can't start (all slots busy) and are queued when the actor exits.
    queued_refs = [a.work.remote(i) for i in range(3)]

    # Release the blocking tasks; they call exit_actor() and the actor exits.
    ray.get(signal_actor.send.remote())

    # Tasks that called exit_actor() and queued (never-started) tasks all fail.
    for ref in blocking_refs + queued_refs:
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(ref, timeout=30)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Graceful shutdown draining is unreliable on Windows.",
)
def test_exit_actor_drains_inflight_across_concurrency_groups(ray_start_regular_shared):
    """exit_actor() on an async actor drains in-flight tasks running in *other*
    concurrency groups before the actor exits.

    Tasks in flight when exit_actor() is called (here in the "io" and "compute"
    groups, while exit() runs in the default group) run to completion and
    deliver their results.
    """
    signal_actor = SignalActor.remote()
    num_io_tasks = 2

    @ray.remote(concurrency_groups={"io": num_io_tasks, "compute": 1})
    class A:
        async def getpid(self):
            return os.getpid()

        async def exit(self):
            ray.actor.exit_actor()

        @ray.method(concurrency_group="io")
        async def wait_then_return(self, value):
            await signal_actor.wait.remote()
            return value

        @ray.method(concurrency_group="compute")
        async def wait_then_raise(self):
            await signal_actor.wait.remote()
            raise ValueError("application error")

    a = A.remote()
    pid = ray.get(a.getpid.remote())

    # Start in-flight tasks in two different concurrency groups; all block on
    # the signal so they are still executing when exit_actor() is called.
    return_refs = [a.wait_then_return.remote(i) for i in range(num_io_tasks)]
    raise_ref = a.wait_then_raise.remote()
    num_inflight = num_io_tasks + 1
    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == num_inflight
    )

    # Request exit from the default concurrency group (which has a free slot)
    # while the io/compute tasks are in flight, then unblock them.
    exit_ref = a.exit.remote()
    ray.get(signal_actor.send.remote())

    # In-flight tasks across both concurrency groups deliver their results and
    # errors before the actor exits.
    assert ray.get(return_refs, timeout=30) == list(range(num_io_tasks))
    with pytest.raises(ValueError, match="application error"):
        ray.get(raise_ref, timeout=30)
    # The task that called exit_actor() observes the actor's death.
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(exit_ref, timeout=30)
    # The actor exits after draining (no process leak).
    wait_for_pid_to_exit(pid)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
