import os
import pytest
import sys
import time
import signal

import ray
from ray._private.test_utils import close_common_connections
from ray.exceptions import ActorUnavailableError, RayActorError


@ray.remote
class Counter:
    def __init__(self, init_time_s=0.01) -> None:
        print(f"Counter init! my pid = {os.getpid()}, sleeping {init_time_s}s...")
        time.sleep(init_time_s)
        self.c = 0

    def slow_increment(self, i, secs):
        self.c += i
        print(f"incrementing self.c by {i} to {self.c}")
        time.sleep(secs)
        return self.c

    def getpid(self):
        return os.getpid()

    def read(self):
        return self.c

    def gen_iota(self, n):
        for i in range(n):
            yield i


def call_from(f, source):
    if source == "driver":
        return f()
    elif source == "actor":

        @ray.remote
        class Wrapper:
            def invoke(self):
                f()

        a = Wrapper.remote()
        return ray.get(a.invoke.remote())
    elif source == "task":

        @ray.remote
        def wrapper():
            return f()

        return ray.get(wrapper.remote())
    else:
        raise ValueError(f"unknown {source}")


def sigkill_actor(actor):
    """Sends SIGKILL to an actor's process. The actor must be on the same node, and it
    must has a `getpid` method."""
    pid = ray.get(actor.getpid.remote())
    print(f"killing actor {actor}'s process {pid}")
    os.kill(pid, signal.SIGKILL)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
def test_actor_unavailable_conn_broken(ray_start_regular, caller):
    def body():
        a = Counter.remote()
        assert ray.get(a.slow_increment.remote(2, 0.1)) == 2
        pid = ray.get(a.getpid.remote())
        task = a.slow_increment.remote(3, 5)
        # Break the grpc connection from this process to the actor process. The
        # next `ray.get` call should fail with ActorUnavailableError.
        close_common_connections(pid)
        with pytest.raises(ActorUnavailableError, match="GrpcUnavailable"):
            ray.get(task)
        # Since the remote() call happens *before* the break, the actor did receive the
        # request, so the side effects are observable, and the actor recovered.
        assert ray.get(a.read.remote()) == 5
        assert ray.get(a.slow_increment.remote(4, 0.1)) == 9

        # Break the connection again. This time, the method call happens after the break
        # so it did not reach the actor. The actor is still in the previous state and
        # the side effects are not observable. Regardless, the method call `.remote()`
        # itself won't raise an error.
        close_common_connections(pid)
        task2 = a.slow_increment.remote(5, 0.1)
        with pytest.raises(ActorUnavailableError, match="GrpcUnavailable"):
            ray.get(task2)
        assert ray.get(a.read.remote()) == 9

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
def test_actor_unavailable_restarting(ray_start_regular, caller):
    def body():
        a = Counter.options(max_restarts=1).remote(init_time_s=5)
        assert ray.get(a.slow_increment.remote(2, 0.1)) == 2

        # Kill the actor process. The actor will restart so we get a temporal
        # unavailable.
        sigkill_actor(a)
        with pytest.raises(ActorUnavailableError):
            print(ray.get(a.slow_increment.remote(2, 0.1)))

        # Actor restarting for 5s. In this period, we get a RESTARTING issue.
        with pytest.raises(
            ActorUnavailableError, match="The actor is in RESTARTING state"
        ):
            print(ray.get(a.slow_increment.remote(2, 0.1)))
        time.sleep(6)

        # After actor started, next calls are OK. However the previous actor instance's
        # state is lost.
        total = 0
        for i in range(10):
            total += i
            assert ray.get(a.slow_increment.remote(i, 0.1)) == total

        # Kill the actor again. This time it's not gonna restart so RayActorError.
        sigkill_actor(a)
        with pytest.raises(RayActorError):
            print(ray.get(a.slow_increment.remote(1, 0.1)))

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
def test_actor_unavailable_norestart(ray_start_regular, caller):
    def body():
        a = Counter.remote()
        assert ray.get(a.read.remote()) == 0

        # Kill the actor process. The actor died permanently so RayActorError.
        sigkill_actor(a)
        with pytest.raises(RayActorError):
            print(ray.get(a.read.remote()))

    call_from(body, caller)


@pytest.mark.skip(reason="https://github.com/ray-project/ray/issues/44357")
@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
def test_generators_early_stop_unavailable(ray_start_regular, caller):
    """
    For a streaming generator, if a connection break happens, *some* elements may still
    yield because they are received prior to the break. Then, two things can happen:
    - an early StopIteration, if `next(gen)` is called.
    - a ActorUnavailableError, if `ray.get(obj_ref)` is called.
    """

    def body():
        a = Counter.remote()
        pid = ray.get(a.getpid.remote())
        total = 2000000
        break_at = 5
        gen = a.gen_iota.remote(total)
        obj_refs = []
        i = 0
        for obj_ref in gen:
            obj_refs.append(obj_ref)
            if i == break_at:
                print(f"breaking conns at {i}")
                close_common_connections(pid)
            i += 1
        # StopIteration happened before `total` elements reached.
        # On my laptop, 11120 elements are collected.
        print(f"collected {len(obj_refs)} elements")
        assert len(obj_refs) < total
        with pytest.raises(ActorUnavailableError):
            ray.get(obj_refs)

    call_from(body, caller)


def test_unavailable_then_actor_error(ray_start_regular):
    @ray.remote(max_restarts=-1, max_task_retries=0)
    class SlowCtor:
        """
        An actor that has a slow constructor. It increments the counter in the
        constructor, and if the counter reaches `die_at_count` after this time's
        increment, it dies.

        To precisely control test behavior, sets infinite restarts, no task retries.
        """

        def __init__(self, start_time_s, counter: Counter, die_at_count):
            count = ray.get(counter.read.remote())
            print(f"SlowCtor init! count = {count}, sleeping {start_time_s}s...")
            time.sleep(start_time_s)
            if count >= die_at_count:
                raise ValueError(f"die at count {die_at_count}!")
            else:
                ray.get(counter.slow_increment.remote(1, 0.1))

        def ping(self, name):
            print(f"ping from {name}")
            return f"hello {name}!"

        def getpid(self):
            return os.getpid()

    c = Counter.remote()
    CTOR_SLEEP_TIME_S = 2
    DIE_AT_COUNT = 2
    # The actor spends 2s in the init; it fails after 1 initial start + 1 restart.
    a = SlowCtor.remote(CTOR_SLEEP_TIME_S, c, DIE_AT_COUNT)
    assert ray.get(a.ping.remote("lemon")) == "hello lemon!"

    # Kill the actor process. The actor will restart so we get a temporal unavailable
    # actor for 2s.
    sigkill_actor(a)

    with pytest.raises(ActorUnavailableError, match="GrpcUnavailable"):
        print(ray.get(a.ping.remote("unavailable")))
    # When the actor is restarting, any method call raises ActorUnavailableError.
    with pytest.raises(ActorUnavailableError, match="The actor is in RESTARTING state"):
        print(ray.get(a.ping.remote("unavailable")))

    # waits for the actor to restart
    time.sleep(3)
    assert ray.get(a.ping.remote("ok")) == "hello ok!"

    # Kill the actor again. First ActorUnavailableError in the init sleep; then the init
    # raised exception so RayActorError.
    sigkill_actor(a)
    with pytest.raises(ActorUnavailableError):
        print(ray.get(a.ping.remote("unavailable")))
    time.sleep(3)
    with pytest.raises(RayActorError, match="an error raised in its creation task"):
        print(ray.get(a.ping.remote("actor error")))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
