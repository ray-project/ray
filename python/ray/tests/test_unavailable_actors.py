import os
import pytest
import sys
import time
import signal

import ray
from ray._private.test_utils import close_common_connections
from ray.exceptions import ActorUnavailableError, ActorDiedError
from typing import Tuple


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
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_unavailable_conn_broken(ray_start_regular, caller):
    def body():
        a = Counter.options(max_task_retries=0).remote()
        assert ray.get(a.slow_increment.remote(2, 0.1)) == 2
        pid = ray.get(a.getpid.remote())
        task = a.slow_increment.remote(3, 5)
        # Break the grpc connection from this process to the actor process. The
        # next `ray.get` call should fail with ActorUnavailableError.
        close_common_connections(pid)
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
        close_common_connections(pid)
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
def test_retryable_tasks_conn_broken(ray_start_regular, caller):
    """
    Number of retries = 1
    1. The task is sent to the actor.
    2. The connection is broken, causing the task to fail (ACTOR_UNAVAILABLE). Does not
        consume a retry.
    3. The task is retried, this time the actor sends back the cached reply (OK).

    Actor's perspective: The actor received 2 calls:
    1. (seqno=1) Updated the state, but the reply failed (connection broken).
    2. (seqno=1) Same seqno, replied the cached result. No re-execution.

    The user's python actor received 1 call, so 1 + 2 = 3.

    Caller's perspective: The caller received 3 task:
    1. (seqno=1) ACTOR_UNAVAILABLE. Retries, not comsuming a retry.
    3. (seqno=1) OK. No retry.
    """

    def body():
        a = Counter.remote()
        assert ray.get(a.slow_increment.remote(1, 0.1)) == 1
        pid = ray.get(a.getpid.remote())
        task = a.slow_increment.options(max_task_retries=1).remote(2, secs=5)
        close_common_connections(pid)
        assert ray.get(task) == 3

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_non_retryable_tasks_conn_broken(ray_start_regular, caller):
    """
    Just like `test_retryable_tasks_conn_broken` but not retryable. The task should
    raise ActorUnavailableError.
    """

    def body():
        a = Counter.remote()
        assert ray.get(a.slow_increment.remote(1, 0.1)) == 1
        pid = ray.get(a.getpid.remote())
        task = a.slow_increment.remote(2, secs=5)
        close_common_connections(pid)
        with pytest.raises(
            ActorUnavailableError,
            match="The task may or maynot have been executed on the actor",
        ):
            ray.get(task)

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_retryable_tasks_conn_broken_keep_order(ray_start_regular, caller):
    """
    In the middle of a stream of tasks, the connection is broken. For the tasks that:

    1. Send to the actor before the break, and replied to the caller, order is kept.
    2. Sent to the actor, processed or not, not yet replied to the caller, the task is
        retried out of order.
    3. Sent to the actor after the task, the order is kept.

    At the connection break:

    0 1 2 3 4 5 6 7 8 9
        ^ caller got reply
            ^ sent to the actor

    We ensure:
    - 0,1 are in order.
    - 2,3,4 may be out of order.
    - 5,6,7,8,9 are in order.

    Out of order means they may be executed along with 5,6,7,8,9 in any sequence.
    """

    @ray.remote(max_task_retries=-1)
    class CallLogger:
        def __init__(self) -> None:
            self.calls = []

        def slow_call(self, arg, secs):
            print(f"recording {self.calls} + {arg} for {secs}s")
            time.sleep(secs)
            self.calls.append(arg)

        def getpid(self):
            return os.getpid()

        def read(self):
            return self.calls

    def body():
        a = CallLogger.remote()
        pid = ray.get(a.getpid.remote())

        waits = []
        for i in range(5):
            waits.append(a.slow_call.remote(f"{i}", secs=1))
        ray.get(waits[:2])  # make sure 0,1 are replied to the caller
        close_common_connections(pid)

        # ensure the connection reestablished
        ray.get(a.getpid.remote())

        for i in range(5, 10):
            waits.append(a.slow_call.remote(f"{i}", secs=1))

        ray.get(waits)
        actor_received = ray.get(a.read.remote())
        print(f"actor received: {actor_received}")

        # 0,1 are in order, and executed only once.
        assert actor_received[:2] == ["0", "1"]
        assert not any(x in actor_received[2:] for x in ["0", "1"])

        # 5,6,7,8,9 are in order, and executed only once.
        assert [x for x in actor_received if x in ["5", "6", "7", "8", "9"]] == [
            "5",
            "6",
            "7",
            "8",
            "9",
        ]

        # 2,3,4 may be out of order, and executed more than once. We only assert that
        # they exist in the list.
        assert all(x in actor_received for x in ["2", "3", "4"])

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_retryable_tasks_killed_keep_order(ray_start_regular, caller):
    """
    In the middle of a stream of tasks, the connection is broken. For the tasks that:

    1. Send to the actor before the break, and replied to the caller, order is kept.
    2. Sent to the actor, processed or not, not yet replied to the caller, the task is
        retried out of order.
    3. Sent to the actor after the task, the order is kept.

    At the connection break:

    0 1 2 3 4 5 6 7 8 9
        ^ caller got reply
            ^ sent to the actor

    We ensure:
    - 0,1 are in order.
    - 2,3,4 may be out of order.
    - 5,6,7,8,9 are in order.

    Out of order means they may be executed along with 5,6,7,8,9 in any sequence.
    """

    @ray.remote(max_task_retries=-1, max_restarts=-1)
    class CallLogger:
        def __init__(self) -> None:
            self.calls = []

        def slow_call(self, arg, secs):
            print(f"recording {self.calls} + {arg} for {secs}s")
            time.sleep(secs)
            self.calls.append(arg)

        def getpid(self):
            return os.getpid()

        def read(self):
            return self.calls

    def body():
        a = CallLogger.remote()
        pid = ray.get(a.getpid.remote())

        waits = []
        for i in range(5):
            waits.append(a.slow_call.remote(f"{i}", secs=1))
        ray.get(waits[:2])  # make sure 0,1 are replied to the caller
        os.kill(pid, signal.SIGKILL)

        # ensure the connection reestablished
        ray.get(a.getpid.remote())

        for i in range(5, 10):
            waits.append(a.slow_call.remote(f"{i}", secs=1))

        ray.get(waits)
        actor_received = ray.get(a.read.remote())
        print(f"actor received: {actor_received}")

        # 0,1 are not re-executed in the new actor.
        # 2,3,4 are re-executed in order; After those retry, 5,6,7,8,9 are executed.
        assert actor_received == ["2", "3", "4", "5", "6", "7", "8", "9"]

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_task_retry_by_connections_closed_and_actor_restart(
    ray_start_regular, caller
):
    def body():
        a = Counter.options(max_restarts=-1).remote()
        assert ray.get(a.slow_increment.remote(2, 0.1)) == 2
        pid = ray.get(a.getpid.remote())
        task1 = a.slow_increment.options(max_task_retries=-1).remote(3, 5)

        close_common_connections(pid)
        assert ray.get(task1) == 2 + 3 * 2

        task2 = a.slow_increment.options(max_task_retries=-1).remote(3, 5)
        # Ensure that Actor has received the PushActorTaskReuqest.
        time.sleep(1)
        os.kill(pid, signal.SIGKILL)
        # The 25 seconds here is to be less than the C constant
        # kMaxReorderWaitSeconds, to ensuring that
        # ActorSchedulingQueue's waiting timeout is not triggered.
        assert ray.get(task2, timeout=25) == 3

    call_from(body, caller)


@pytest.mark.parametrize(
    "caller",
    ["actor", "task", "driver"],
)
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
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
