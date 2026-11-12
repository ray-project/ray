"""Unit tests for readiness probes.

Two halves: the :class:`Probe` value object, which needs nothing at all, and
``Sandbox.wait_until_ready()`` driven against a fake actor that counts calls --
so the round trips this path is supposed to avoid are asserted rather than
assumed. Neither half needs runsc or a Ray cluster.
"""

import asyncio
import errno
import functools
import sys

import pytest

import ray
from ray.experimental.sandbox.modal import Probe, Sandbox, _actor
from ray.experimental.sandbox.modal._actor import _SandboxActor
from ray.experimental.sandbox.modal._command import ARG_MAX_BYTES
from ray.experimental.sandbox.modal.exception import (
    ExecutionError,
    InvalidError,
    NotFoundError,
    SandboxTerminatedError,
    SandboxTimeoutError,
    TimeoutError,
)
from ray.experimental.sandbox.modal.sandbox import _Sandbox


class _RemoteShim:
    """Gives a plain coroutine the ``.remote()`` call shape of an actor method."""

    def __init__(self, fn):
        self._fn = fn

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class FakeActor:
    """Stands in for ``_SandboxActor``, returning a canned ``wait_ready``."""

    def __init__(
        self, outcome="ready", exit_reason=None, error=None, probe_stopped=False
    ):
        self._outcome = outcome
        self._exit_reason = exit_reason
        self._error = error
        self._probe_stopped = probe_stopped
        self.wait_ready_calls = []

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _RemoteShim(functools.partial(impl, self))

    async def _wait_ready(self, timeout):
        self.wait_ready_calls.append(timeout)
        return {
            "outcome": self._outcome,
            "exit_reason": self._exit_reason,
            "error": self._error,
            "probe_stopped": self._probe_stopped,
        }


def make_sandbox(actor=None, probe=Probe.with_exec("true")):
    return Sandbox._from_impl(_Sandbox(actor, "ray-sandbox-test", None, probe))


# -- the Probe value object ------------------------------------------------


# Modal's Probe is a plain dataclass, so building one without the `with_*`
# helpers is part of the surface -- and the actor's probe loop cannot report a
# malformed probe usefully: it would only surface as an attempt that keeps
# failing to run. So both routes refuse it at the line that built the probe.
_ROUTES = {
    "with_exec": lambda argv, interval_ms: Probe.with_exec(
        *argv, interval_ms=interval_ms
    ),
    "direct": lambda argv, interval_ms: Probe(exec_argv=argv, interval_ms=interval_ms),
}
_EITHER_ROUTE = pytest.mark.parametrize("route", sorted(_ROUTES))


@_EITHER_ROUTE
@pytest.mark.parametrize(
    "argv",
    [(), (b"bytes",), ("ok", 5), (None,), ("x" * (ARG_MAX_BYTES + 1),)],
)
def test_a_probe_holds_argv_to_the_same_rules_as_exec(route, argv):
    with pytest.raises(InvalidError):
        _ROUTES[route](argv, 100)


@_EITHER_ROUTE
@pytest.mark.parametrize("interval_ms", [0, -1, 1.5, "100", True, None])
def test_a_probe_refuses_an_unusable_interval(route, interval_ms):
    with pytest.raises(InvalidError):
        _ROUTES[route](("true",), interval_ms)


@pytest.mark.parametrize(
    "build", [lambda: Probe(tcp_port=8080), lambda: Probe.with_tcp(8080)]
)
def test_a_tcp_probe_is_refused_whichever_route_built_it(build):
    """Unimplemented until the backend grows port forwarding. Naming only
    tcp_port satisfies the "exactly one" rule, so it used to be accepted and
    then failed inside the actor on ``list(None)``."""
    with pytest.raises(NotImplementedError, match="TCP readiness.*Probe.with_exec"):
        build()


@pytest.mark.parametrize("kwargs", [{}, {"tcp_port": 1, "exec_argv": ("true",)}])
def test_a_probe_must_name_exactly_one_check(kwargs):
    with pytest.raises(InvalidError, match="exactly one"):
        Probe(**kwargs)


# -- wait_until_ready ------------------------------------------------------


def test_without_a_probe_it_is_invalid_and_costs_no_rpc():
    actor = FakeActor()
    sandbox = make_sandbox(actor, probe=None)
    with pytest.raises(InvalidError):
        sandbox.wait_until_ready()
    assert actor.wait_ready_calls == []


def test_readiness_is_cached_so_a_second_call_costs_no_rpc():
    """Readiness is monotonic, so only the first call goes over the wire."""
    actor = FakeActor("ready")
    sandbox = make_sandbox(actor)
    sandbox.wait_until_ready()
    sandbox.wait_until_ready()
    assert len(actor.wait_ready_calls) == 1


def test_a_probe_timeout_is_modals_timeout_error_and_not_terminal():
    """Not SandboxTimeoutError, though Modal's hierarchy puts both under
    TimeoutError: a probe that did not pass says nothing about the Sandbox
    having exceeded its own lifetime. Nor is it a terminal state, so the caller
    may reasonably wait again with a longer timeout."""
    actor = FakeActor("timeout")
    sandbox = make_sandbox(actor)
    with pytest.raises(TimeoutError) as excinfo:
        sandbox.wait_until_ready(timeout=1)
    assert not isinstance(excinfo.value, SandboxTimeoutError)
    with pytest.raises(TimeoutError):
        sandbox.wait_until_ready(timeout=2)
    assert actor.wait_ready_calls == [1, 2]


@pytest.mark.parametrize(
    "exit_reason,expected",
    [
        ("terminated", SandboxTerminatedError),
        ("timeout", SandboxTimeoutError),
        (None, SandboxTerminatedError),
    ],
)
def test_a_sandbox_that_ended_first_reports_why_without_a_second_rpc(
    exit_reason, expected
):
    """The exit reason rides back with the outcome, so this needs no get_state.
    FakeActor has no get_state at all, so reaching for one would AttributeError."""
    actor = FakeActor("ended", exit_reason=exit_reason)
    with pytest.raises(expected):
        make_sandbox(actor).wait_until_ready()
    assert len(actor.wait_ready_calls) == 1


def test_a_probe_that_could_not_run_is_an_execution_error_not_a_termination():
    """The probe failing to run says nothing about the Sandbox, which may well
    still be running: this used to raise SandboxTerminatedError."""
    actor = FakeActor("error", error="BlockingIOError: [Errno 11] fork failed")
    with pytest.raises(ExecutionError, match="probe could not run.*fork failed"):
        make_sandbox(actor).wait_until_ready()


class _ActorDiesDuringWait(FakeActor):
    """wait_ready dies under the caller, as a killed actor's call does."""

    def __init__(self, terminated_here):
        super().__init__()
        self.terminated_here = terminated_here
        self.sandbox = None

    async def _wait_ready(self, timeout):
        if self.terminated_here:
            # What terminate() on this same handle does, from another thread,
            # before it kills the actor.
            self.sandbox._actor_released = True
        raise ray.exceptions.ActorDiedError()


@pytest.mark.parametrize(
    "terminated_here, expected",
    [(False, NotFoundError), (True, SandboxTerminatedError)],
)
def test_an_actor_dying_mid_wait_is_a_termination_only_if_terminate_killed_it(
    terminated_here, expected
):
    """An actor that died on its own -- its node lost, Ray reclaiming it -- is
    a gone Sandbox, as wait() and poll() report it: this used to say it was
    terminated."""
    actor = _ActorDiesDuringWait(terminated_here)
    impl = _Sandbox(actor, "ray-sandbox-test", None, Probe.with_exec("true"))
    actor.sandbox = impl
    with pytest.raises(expected):
        Sandbox._from_impl(impl).wait_until_ready()


def test_a_probe_that_stopped_says_so_rather_than_blaming_the_timeout():
    """Once the probe's window has closed, every wait returns at once: saying
    it did not become ready "within 600 seconds" was untrue."""
    actor = FakeActor("timeout", probe_stopped=True)
    with pytest.raises(TimeoutError, match="probe stopped after 300 seconds"):
        make_sandbox(actor).wait_until_ready(timeout=600)


# -- the probe loop, inside the actor --------------------------------------

# The real class behind @ray.remote, so the loop can be driven on a plain event
# loop with no cluster and no runsc.
SandboxActor = _SandboxActor.__ray_metadata__.modified_class


MAIN_COMMAND = ["sleep", "3600"]
PROBE_COMMAND = "true"


def make_actor(
    probe=Probe.with_exec(PROBE_COMMAND, interval_ms=1),
    returncodes=(0,),
    exec_start=None,
):
    """An actor whose exec calls are stubbed with canned exit codes.

    The last code repeats, so a probe that never passes is ``(1,)``. Every exec
    is recorded in ``execs``; ``attempts`` is the probe's share of them, which
    is what the tests care about -- the main process goes through the same
    stub but is not a probe attempt.
    """
    actor = SandboxActor({}, None, None, probe)
    actor.execs = []
    actor.released = []
    pending = list(returncodes)

    async def record(argv, **kwargs):
        actor.execs.append((argv, kwargs))
        exec_id = f"exec-{len(actor.execs)}"
        if exec_start is not None and argv != MAIN_COMMAND:
            return await exec_start(exec_id, argv, **kwargs)
        return exec_id

    async def exec_wait(exec_id, timeout=None):
        return pending.pop(0) if len(pending) > 1 else pending[0]

    async def exec_release(exec_id):
        actor.released.append(exec_id)

    actor.exec_start = record
    actor.exec_wait = exec_wait
    actor.exec_release = exec_release
    return actor


def attempts(actor):
    """The probe's execs, dropping the main process's."""
    return [(argv, kwargs) for argv, kwargs in actor.execs if argv != MAIN_COMMAND]


def test_the_probe_runs_the_command_until_it_exits_zero():
    actor = make_actor(returncodes=(1, 1, 0))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        return await actor.wait_ready(5)

    result = asyncio.run(scenario())
    assert result["outcome"] == "ready"
    # Two failures then the success.
    assert len(attempts(actor)) == 3
    argv, kwargs = attempts(actor)[0]
    assert argv == [PROBE_COMMAND]
    # Nothing ever reads a probe's output, and no attempt may hang the loop.
    assert kwargs == {
        "stdout_devnull": True,
        "stderr_devnull": True,
        "timeout": _actor._PROBE_ATTEMPT_TIMEOUT_SECONDS,
    }


def test_every_attempt_is_released_so_it_cannot_evict_real_exec_output():
    """exec_wait only retires a session into the bounded retention window; at a
    100ms interval the probe would cycle that window in seconds and drop the
    output of the caller's own execs."""
    actor = make_actor(returncodes=(1, 1, 1, 0))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        await actor.wait_ready(5)

    asyncio.run(scenario())
    # exec-1 is the main process; every probe attempt after it is released.
    assert actor.released == ["exec-2", "exec-3", "exec-4", "exec-5"]


def test_no_probe_means_no_task_and_no_attempts():
    actor = make_actor(probe=None)

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        assert actor._probe_task is None

    asyncio.run(scenario())
    assert attempts(actor) == []


def test_wait_ready_starts_the_probe_when_there_is_no_main_process():
    """A Sandbox created with no command never calls launch_main, so the probe
    has to start on demand or such a caller would block forever."""
    actor = make_actor(returncodes=(0,))
    result = asyncio.run(actor.wait_ready(5))
    assert result["outcome"] == "ready"
    assert len(attempts(actor)) == 1


def test_probing_stops_when_modals_window_closes(monkeypatch):
    """Modal probes for five minutes at most, then wait_until_ready() times
    out whatever timeout it was given. The loop used to run until the
    sandbox ended -- an exec every interval, for up to 24 hours."""
    monkeypatch.setattr(_actor, "_PROBE_WINDOW_SECONDS", 0.05)
    actor = make_actor(returncodes=(1,))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        first = await actor.wait_ready(5)
        tried = len(attempts(actor))
        await asyncio.sleep(0.05)
        return first, tried, len(attempts(actor)), await actor.wait_ready(5)

    first, tried, later, again = asyncio.run(scenario())
    assert first["outcome"] == "timeout"
    assert later == tried, "no attempts after the window closed"
    # Later callers get the same answer at once rather than waiting, and are
    # told it is the probe that stopped rather than their own wait.
    assert again["outcome"] == "timeout"
    assert first["probe_stopped"] and again["probe_stopped"]
    # Each attempt is bounded by the window as well as by its own limit.
    assert all(kw["timeout"] <= 0.05 for _, kw in attempts(actor))


def test_teardown_releases_a_parked_waiter_rather_than_stranding_it():
    actor = make_actor(returncodes=(1,))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        actor._exit_reason = "terminated"
        waiter = asyncio.ensure_future(actor.wait_ready(30))
        await asyncio.sleep(0)
        await actor._teardown()
        # Bounded by the wait_ready timeout only if teardown failed to wake it;
        # this asserts it is woken promptly instead.
        return await asyncio.wait_for(waiter, 5)

    result = asyncio.run(scenario())
    assert result["outcome"] == "ended"
    assert result["exit_reason"] == "terminated"


def test_teardown_does_not_overwrite_a_probe_that_already_passed():
    actor = make_actor(returncodes=(0,))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        assert (await actor.wait_ready(5))["outcome"] == "ready"
        await actor._teardown()
        return await actor.wait_ready(5)

    assert asyncio.run(scenario())["outcome"] == "ready"


def _refuse_fork():
    """Raise what spawning an attempt does when the host cannot fork."""
    raise BlockingIOError(errno.EAGAIN, "Resource temporarily unavailable")


async def _launch_and_wait_ready(actor):
    await actor.launch_main(MAIN_COMMAND)
    return await actor.wait_ready(5)


def test_a_wait_that_runs_out_first_is_not_a_stopped_probe():
    actor = make_actor(returncodes=(1,))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        return await actor.wait_ready(0.05)

    result = asyncio.run(scenario())
    assert (result["outcome"], result["probe_stopped"]) == ("timeout", False)


def test_a_probe_that_cannot_run_reports_an_error_not_an_ended_sandbox():
    """An attempt that raises -- here a fork the host refused -- used to be
    recorded as "ended", and wait_until_ready() then reported a running
    Sandbox as terminated. (A command missing from the image never got there:
    runsc exits 128, an ordinary failed attempt.)"""

    async def refused(exec_id, argv, **kwargs):
        _refuse_fork()

    actor = make_actor(returncodes=(1,), exec_start=refused)
    result = asyncio.run(_launch_and_wait_ready(actor))
    assert result["outcome"] == "error"
    assert result["exit_reason"] is None, "the sandbox never stopped"
    assert result["error"].startswith("BlockingIOError")
    assert len(attempts(actor)) == _actor._PROBE_ERROR_ATTEMPTS


def test_an_attempt_that_could_not_run_once_is_retried():
    """A blip -- one refused fork -- is a failed attempt, not the end of the
    probe."""
    calls = []

    async def refused_once(exec_id, argv, **kwargs):
        calls.append(exec_id)
        if len(calls) == 1:
            _refuse_fork()
        return exec_id

    actor = make_actor(returncodes=(0,), exec_start=refused_once)
    assert asyncio.run(_launch_and_wait_ready(actor))["outcome"] == "ready"
    assert len(attempts(actor)) == 2


def test_an_attempt_that_fails_because_the_sandbox_ended_reports_it_ended():
    """That one is "ended", and the exit reason travels with it."""

    async def sandbox_went(exec_id, argv, **kwargs):
        actor._exit_reason = "terminated"
        raise NotFoundError("The Sandbox is unavailable: it was terminated.")

    actor = make_actor(returncodes=(1,), exec_start=sandbox_went)
    result = asyncio.run(_launch_and_wait_ready(actor))
    assert (result["outcome"], result["exit_reason"]) == ("ended", "terminated")
    assert len(attempts(actor)) == 1


def test_a_sandbox_that_ends_after_its_probe_failed_reports_the_ending():
    """Once the Sandbox has stopped, that is what a later wait reports."""

    async def refused(exec_id, argv, **kwargs):
        _refuse_fork()

    actor = make_actor(returncodes=(1,), exec_start=refused)

    async def scenario():
        first = await _launch_and_wait_ready(actor)
        await actor.terminate()
        return first, await actor.wait_ready(5)

    first, after = asyncio.run(scenario())
    assert first["outcome"] == "error"
    assert (after["outcome"], after["exit_reason"]) == ("ended", "terminated")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
