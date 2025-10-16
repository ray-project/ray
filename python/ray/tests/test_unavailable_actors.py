import os
import signal
import sys
from typing import Optional, Tuple

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.exceptions import ActorDiedError, ActorUnavailableError

import psutil  # We must import psutil after ray because we bundle it with ray.


@ray.remote(num_cpus=0)
class Counter:
    def __init__(
        self,
        *,
        caller_pid: Optional[int] = None,
        init_signal: Optional[ray.actor.ActorHandle] = None,
    ):
        if init_signal is not None:
            ray.get(init_signal.wait.remote())

        self._count = 0
        self._caller_pid = caller_pid

    def getpid(self):
        return os.getpid()

    def get(self) -> int:
        return self._count

    def inc(self, *, disconnect: bool = False) -> int:
        if disconnect:
            assert self._caller_pid is not None, "Must provide caller PID."
            _close_common_connections(self._caller_pid)

        self._count += 1
        return self._count


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


def sigkill_actor(actor, timeout=5):
    """Sends SIGKILL to an actor's process. The actor must be on the same node, and it
    must has a `getpid` method."""
    pid = ray.get(actor.getpid.remote())
    print(f"killing actor {actor}'s process {pid}")
    try:
        proc = psutil.Process(pid)
        os.kill(pid, signal.SIGKILL)

        # Wait for the process to terminate (with timeout)
        try:
            proc.wait(timeout=timeout)
            print(f"Process {pid} terminated.")
        except psutil.TimeoutExpired:
            print(f"Process {pid} did not terminate within {timeout} seconds.")
    except psutil.NoSuchProcess:
        print(f"Process {pid} does not exist — it may have already exited.")


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


@pytest.mark.parametrize("caller", ["actor", "task", "driver"])
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_unavailable_conn_broken(ray_start_regular, caller):
    def _run_test():
        a = Counter.remote(caller_pid=os.getpid())
        counter_pid = ray.get(a.getpid.remote())

        # Server (counter actor) unexpectedly disconnects the connection once the method
        # has started executing. The task should raise `ActorUnavailableError` but its
        # side effects should be observable (count was incremented).
        obj_ref = a.inc.remote(disconnect=True)
        with pytest.raises(ActorUnavailableError):
            ray.get(obj_ref)
        assert ray.get(a.get.remote()) == 1

        # Client (driver) unexpectedly disconnects the connection prior to submitting the
        # task. The task should raise `ActorUnavailableError` and should never have
        # executed, therefore the count should not have been incremented.
        _close_common_connections(counter_pid)
        with pytest.raises(ActorUnavailableError):
            ray.get(a.inc.remote(disconnect=False))
        assert ray.get(a.get.remote()) == 1

    call_from(_run_test, caller)


@pytest.mark.parametrize("caller", ["actor", "task", "driver"])
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_unavailable_restarting(ray_start_regular, caller):
    def _run_test():
        init_signal = SignalActor.remote()
        a = Counter.options(max_restarts=1).remote(init_signal=init_signal)
        wait_for_condition(lambda: ray.get(init_signal.cur_num_waiters.remote()) == 1)
        ray.get(init_signal.send.remote(clear=True))
        assert ray.get(a.inc.remote()) == 1

        # Kill the actor process and expect `ActorUnavailableError` as it restarts.
        sigkill_actor(a)
        with pytest.raises(ActorUnavailableError):
            ray.get(a.inc.remote())

        with pytest.raises(ActorUnavailableError, match="The actor is restarting"):
            ray.get(a.inc.remote())

        ray.get(init_signal.send.remote())

        # After the actor starts, the next calls are OK. However the previous actor
        # instance's state is lost.
        wait_for_condition(lambda: ray.get(a.get.remote()) == 0)

        # Kill the actor again. This time it's not going to restart so ActorDiedError.
        sigkill_actor(a)
        with pytest.raises(ActorDiedError):
            print(ray.get(a.inc.remote()))

    call_from(_run_test, caller)


@pytest.mark.parametrize("caller", ["actor", "task", "driver"])
@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_unavailable_norestart(ray_start_regular, caller):
    def _run_test():
        a = Counter.remote()
        assert ray.get(a.get.remote()) == 0

        # Kill the actor process. The actor died permanently so ActorDiedError.
        sigkill_actor(a)
        with pytest.raises(ActorDiedError):
            print(ray.get(a.get.remote()))

    call_from(_run_test, caller)


@ray.remote(max_restarts=-1, max_task_retries=0)
class ActorAwaitingOnCreation:
    """
    An actor that is awaiting for a signal to be sent to it during its init. It is used
    to test the behavior of the actor when it is killed and restarted.

    It also increments a counter during its init to keep track of the number of
    restarts.
    """

    def __init__(
        self,
        restart_counter: Counter,
        blocking_signal: SignalActor,
        restart_death_range: Tuple[int, int],
    ):
        restart_count = ray.get(restart_counter.inc.remote())
        ray.get(blocking_signal.wait.remote())  # block on signal
        restart_death_lower, restart_death_upper = restart_death_range
        if restart_count > restart_death_lower and restart_count < restart_death_upper:
            msg = (
                f"Failed to restart the actor because the restart count is in the death range [{restart_death_lower}, "
                f"{restart_death_upper}]: {restart_count}"
            )
            print(msg)
            raise ValueError(msg)

    def ping(self, name):
        print(f"ping from {name}")
        return f"hello {name}!"

    def getpid(self):
        return os.getpid()


@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_restart(ray_start_regular):
    """
    Test the following actor restart scenarios:
    - The actor restarts successfully on being killed.
    - The actor emits the right error message during the restart when it is not fully
      initialized.
    - The actor emits the right error message when it is permanently dead.
    """
    counter = Counter.remote()
    signal_actor = SignalActor.remote()
    actor = ActorAwaitingOnCreation.options(max_restarts=3).remote(
        restart_counter=counter,
        blocking_signal=signal_actor,
        restart_death_range=(2, 10),
    )

    # unblock actor creation, actor should be created eventually
    ray.get(signal_actor.send.remote())
    wait_for_condition(
        lambda: ray.get(actor.ping.remote("lemon")) == "hello lemon!",
    )

    # block actor creation and kill it
    ray.get(signal_actor.send.remote(clear=True))
    sigkill_actor(actor)

    with pytest.raises(ActorUnavailableError):
        print(ray.get(actor.ping.remote("unavailable")))

    # unblock actor creation, actor should be created eventually
    ray.get(signal_actor.send.remote())
    wait_for_condition(
        lambda: ray.get(actor.ping.remote("ok")) == "hello ok!",
    )

    # block actor creation and kill it
    ray.get(signal_actor.send.remote(clear=True))
    sigkill_actor(actor)

    with pytest.raises(ActorUnavailableError):
        print(ray.get(actor.ping.remote("unavailable")))

    # unblock actor creation, the actor still dies because it reaches the restart limit
    ray.get(signal_actor.send.remote())
    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 0,
    )
    with pytest.raises(ActorDiedError, match="an error raised in its creation task"):
        print(ray.get(actor.ping.remote("actor error")))


@pytest.mark.skipif(sys.platform == "win32", reason="does not work on windows")
def test_actor_inifite_restart(ray_start_regular):
    """
    Test that the actor can be restarted inifinitely. We do that by intentionally
    cause the actor to fail when its restarting counter is in the death range. We
    then test that the restarting counter will eventually go out of the death range
    and the actor will be able to restart.
    """
    counter = Counter.remote()
    signal_actor = SignalActor.remote()
    actor = ActorAwaitingOnCreation.options().remote(
        restart_counter=counter,
        blocking_signal=signal_actor,
        restart_death_range=(2, 5),
    )

    # unblock actor creation
    ray.get(signal_actor.send.remote())
    wait_for_condition(
        lambda: ray.get(actor.ping.remote("lemon")) == "hello lemon!",
    )

    # block actor creation and kill it
    ray.get(signal_actor.send.remote(clear=True))
    sigkill_actor(actor)
    # When the actor is restarting, any method call raises ActorUnavailableError.
    with pytest.raises(ActorUnavailableError):
        print(ray.get(actor.ping.remote("unavailable")))
    # unblock actor creation, the actor keeps retrying until it gets out of the death
    # range
    ray.get(signal_actor.send.remote())
    assert ray.get(actor.ping.options(max_task_retries=-1).remote("ok")) == "hello ok!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
