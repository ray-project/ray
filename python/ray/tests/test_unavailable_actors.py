import os
import pytest
import sys
import time
import signal
from typing import Tuple

import ray
from ray.exceptions import ActorUnavailableError, ActorDiedError

import psutil  # We must import psutil after ray because we bundle it with ray.


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


def _close_common_connections(pid: int):
    """Closes ipv2 connections between the current process and the target process."""
    current_process = psutil.Process()
    current_connections = current_process.connections(kind="inet")
    try:
        other_process = psutil.Process(pid)
        other_connections = other_process.connections(kind="inet")
    except psutil.NoSuchProcess:
        print(f"No process with PID {pid} found.")
        return
    # Finding common connections based on matching addresses and ports.
    common_connections = []
    for conn1 in current_connections:
        for conn2 in other_connections:
            if conn1.laddr == conn2.raddr and conn1.raddr == conn2.laddr:
                common_connections.append((conn1.fd, conn1.laddr, conn1.raddr))
    # Closing the FDs.
    for fd, laddr, raddr in common_connections:
        if fd != -1:  # FD is -1 if it's not accessible or if it's a pseudo FD.
            os.close(fd)
            print(f"Closed FD: {fd}, laddr: {laddr}, raddr: {raddr}")


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
@pytest.mark.parametrize("ray_start_regular", [{"log_to_driver": False}], indirect=True)
def test_actor_unavailable_conn_broken(ray_start_regular, caller):
    def body():
        a = Counter.remote()
        assert ray.get(a.slow_increment.remote(2, 0.1)) == 2
        pid = ray.get(a.getpid.remote())
        task = a.slow_increment.remote(3, 5)
        # Break the grpc connection from this process to the actor process. The
        # next `ray.get` call should fail with ActorUnavailableError.
        _close_common_connections(pid)
        with pytest.raises(ActorUnavailableError, match="RpcError"):
            ray.get(task)
        # Since the remote() call happens *before* the break, the actor did receive the
        # request, so the side effects are observable, and the actor recovered.
        assert ray.get(a.read.remote()) == 5
        assert ray.get(a.slow_increment.remote(4, 0.1)) == 9

        # Break the connection again. This time, the method call happens after the break
        # so it did not reach the actor. The actor is still in the previous state and
        # the side effects are not observable. Regardless, the method call `.remote()`
        # itself won't raise an error.
        _close_common_connections(pid)
        task2 = a.slow_increment.remote(5, 0.1)
        with pytest.raises(ActorUnavailableError, match="RpcError"):
            ray.get(task2)
        assert ray.get(a.read.remote()) == 9

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
@pytest.mark.parametrize("ray_start_regular", [{"log_to_driver": False}], indirect=True)
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
        with pytest.raises(ActorUnavailableError, match="The actor is restarting"):
            print(ray.get(a.slow_increment.remote(2, 0.1)))
        time.sleep(6)

        # After the actor starts, the next calls are OK. However the previous actor
        # instance's state is lost.
        total = 0
        for i in range(10):
            total += i
            assert ray.get(a.slow_increment.remote(i, 0.1)) == total

        # Kill the actor again. This time it's not going to restart so ActorDiedError.
        sigkill_actor(a)
        with pytest.raises(ActorDiedError):
            print(ray.get(a.slow_increment.remote(1, 0.1)))

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
@pytest.mark.parametrize("ray_start_regular", [{"log_to_driver": False}], indirect=True)
def test_actor_unavailable_norestart(ray_start_regular, caller):
    def body():
        a = Counter.remote()
        assert ray.get(a.read.remote()) == 0

        # Kill the actor process. The actor died permanently so ActorDiedError.
        sigkill_actor(a)
        with pytest.raises(ActorDiedError):
            print(ray.get(a.read.remote()))

    call_from(body, caller)


@ray.remote(max_restarts=-1, max_task_retries=0)
class SlowCtor:
    """
    An actor that has a slow init. It performs:

    1. sleeps for `init_sleep_s`,
    2. increments the counter in the init,
    3. if the counter value before increment is within the `die_range`, raises error.

    To precisely control test behavior, sets infinite restarts, no task retries.
    """

    def __init__(self, counter: Counter, init_sleep_s, die_range: Tuple[int, int]):
        count = ray.get(counter.slow_increment.remote(1, 0.1))
        count -= 1  # we want the count before increment
        print(f"SlowCtor init! count = {count}, sleeping {init_sleep_s}s...")
        time.sleep(init_sleep_s)
        if die_range[0] <= count and count < die_range[1]:
            msg = (
                f"die at count {count} because it's in range"
                f" [{die_range[0]}, {die_range[1]})!"
            )
            print(msg)
            raise ValueError(msg)

    def ping(self, name):
        print(f"ping from {name}")
        return f"hello {name}!"

    def getpid(self):
        return os.getpid()


@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
@pytest.mark.parametrize("ray_start_regular", [{"log_to_driver": False}], indirect=True)
def test_unavailable_then_actor_error(ray_start_regular):
    c = Counter.remote()
    # Restart config:
    # Initial run, Restart #1: ok.
    # Restart #2, #3: fails, can retry.
    # Afterwards: no more restarts, any new method call raises ActorDiedError.
    a = SlowCtor.options(max_restarts=3).remote(
        counter=c, init_sleep_s=2, die_range=[2, 10000]
    )
    assert ray.get(a.ping.remote("lemon")) == "hello lemon!"

    # Kill the actor process. Triggers restart #1. During its __init__, all incoming
    # calls get ActorUnavailableError.
    sigkill_actor(a)

    with pytest.raises(ActorUnavailableError, match="RpcError"):
        print(ray.get(a.ping.remote("unavailable")))
    # When the actor is restarting, any method call raises ActorUnavailableError.
    with pytest.raises(ActorUnavailableError, match="The actor is restarting"):
        print(ray.get(a.ping.remote("unavailable")))

    # Waits for the actor to restart.
    time.sleep(3)
    assert ray.get(a.ping.remote("ok")) == "hello ok!"

    # Kill the actor again. Triggers restart #2. However it raises ValueError in init
    # so it consequently triggers restart #3 and also fails. Afterwards, ActorDiedError
    # is raised.
    sigkill_actor(a)
    with pytest.raises(ActorUnavailableError):
        print(ray.get(a.ping.remote("unavailable")))
    time.sleep(4)
    with pytest.raises(ActorDiedError, match="an error raised in its creation task"):
        print(ray.get(a.ping.remote("actor error")))


@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
@pytest.mark.parametrize("ray_start_regular", [{"log_to_driver": False}], indirect=True)
def test_inf_task_retries(ray_start_regular):
    c = Counter.remote()
    # The actor spends 2s in the init.
    # Initial start and restart #1 succeeds, but restarts #2, #3, #4 fails. Then all
    # later restarts succeeds.
    a = SlowCtor.remote(counter=c, init_sleep_s=2, die_range=[2, 5])
    assert ray.get(a.ping.remote("lemon")) == "hello lemon!"

    # Kill the actor process. Triggers restart #1. During the init a remote call gets
    # ActorUnavailableError, and after the init, the actor can receive tasks.
    sigkill_actor(a)
    # Actor is restarting, any method call raises ActorUnavailableError.
    with pytest.raises((ActorUnavailableError)):
        ray.get(a.ping.remote("unavailable"))
    # But if the task has retries, it retries until the actor is available.
    # Each retry happens after RAY_task_retry_delay_ms (default 0) wait.
    # On my laptop, it took 8 retries for the 2s actor init time.
    assert (
        ray.get(a.ping.options(max_task_retries=-1).remote("retry")) == "hello retry!"
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
