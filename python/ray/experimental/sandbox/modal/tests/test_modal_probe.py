"""Unit tests for readiness probes.

Two halves: the :class:`Probe` value object, which needs nothing at all, and
``Sandbox.wait_until_ready()`` driven against a fake actor that counts calls --
so the round trips this path is supposed to avoid are asserted rather than
assumed. Neither half needs runsc or a Ray cluster.
"""

import asyncio
import functools
import inspect
import sys

import pytest

from ray.experimental.sandbox.modal import Probe, Sandbox, _actor
from ray.experimental.sandbox.modal._actor import _SandboxActor
from ray.experimental.sandbox.modal._command import ARG_MAX_BYTES
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    SandboxTerminatedError,
    SandboxTimeoutError,
    TimeoutError,
)
from ray.experimental.sandbox.modal.probe import DEFAULT_INTERVAL_MS
from ray.experimental.sandbox.modal.sandbox import _Sandbox


class _RemoteShim:
    """Gives a plain coroutine the ``.remote()`` call shape of an actor method."""

    def __init__(self, fn):
        self._fn = fn

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class FakeActor:
    """Stands in for ``_SandboxActor``, returning a canned ``wait_ready``."""

    def __init__(self, outcome="ready", exit_reason=None):
        self._outcome = outcome
        self._exit_reason = exit_reason
        self.wait_ready_calls = []

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _RemoteShim(functools.partial(impl, self))

    async def _wait_ready(self, timeout):
        self.wait_ready_calls.append(timeout)
        return {"outcome": self._outcome, "exit_reason": self._exit_reason}


def make_sandbox(actor=None, probe=Probe.with_exec("true")):
    return Sandbox._from_impl(_Sandbox(actor, "ray-sandbox-test", None, probe))


# -- the Probe value object ------------------------------------------------


def test_with_exec_records_the_command_and_interval():
    probe = Probe.with_exec("sh", "-c", "test -f /tmp/ready", interval_ms=250)
    assert probe.exec_argv == ("sh", "-c", "test -f /tmp/ready")
    assert probe.interval_ms == 250
    assert probe.tcp_port is None


def test_with_exec_defaults_to_modals_interval():
    assert Probe.with_exec("true").interval_ms == DEFAULT_INTERVAL_MS == 100


@pytest.mark.parametrize(
    "argv",
    [(), (b"bytes",), ("ok", 5), (None,), ("x" * (ARG_MAX_BYTES + 1),)],
)
def test_with_exec_holds_argv_to_the_same_rules_as_exec(argv):
    with pytest.raises(InvalidError):
        Probe.with_exec(*argv)


@pytest.mark.parametrize("interval_ms", [0, -1, 1.5, "100", True, None])
def test_with_exec_refuses_an_unusable_interval(interval_ms):
    with pytest.raises(InvalidError):
        Probe.with_exec("true", interval_ms=interval_ms)


# -- direct construction ---------------------------------------------------
#
# Modal's Probe is a plain dataclass, so building one without the `with_*`
# helpers is part of the surface -- and the actor's probe loop cannot report a
# malformed probe. Whatever it raises is swallowed by its catch-all and
# recorded as the probe having ended, which makes wait_until_ready() report a
# perfectly healthy Sandbox as terminated. Everything here is refused at the
# line that built the probe instead.


def test_a_tcp_probe_is_refused_at_construction():
    """Naming only tcp_port satisfies the "exactly one" rule, so it used to be
    accepted and then failed inside the actor on ``list(None)``."""
    with pytest.raises(NotImplementedError, match="TCP readiness probes"):
        Probe(tcp_port=8080)


def test_a_tcp_probe_is_refused_the_same_way_whichever_route_built_it():
    direct = pytest.raises(NotImplementedError, match="Probe.with_exec")
    with direct:
        Probe(tcp_port=8080)
    with direct:
        Probe.with_tcp(8080)


@pytest.mark.parametrize("kwargs", [{}, {"tcp_port": 1, "exec_argv": ("true",)}])
def test_a_probe_must_name_exactly_one_check(kwargs):
    with pytest.raises(InvalidError, match="exactly one"):
        Probe(**kwargs)


@pytest.mark.parametrize("argv", [(), (b"bytes",), ("ok", 5), (None,)])
def test_direct_construction_holds_argv_to_the_same_rules(argv):
    with pytest.raises(InvalidError):
        Probe(exec_argv=argv)


@pytest.mark.parametrize("interval_ms", [0, -1, 1.5, "100", True, None])
def test_direct_construction_refuses_an_unusable_interval(interval_ms):
    with pytest.raises(InvalidError):
        Probe(exec_argv=("true",), interval_ms=interval_ms)


def test_probe_is_immutable():
    probe = Probe.with_exec("true")
    with pytest.raises(Exception):
        probe.interval_ms = 5


def test_with_tcp_is_refused():
    """Unimplemented until the backend grows port forwarding. Asserted through
    the builtin so this still passes once with_tcp starts working."""
    with pytest.raises(NotImplementedError):
        Probe.with_tcp(8080)


# -- wait_until_ready ------------------------------------------------------


def test_without_a_probe_it_is_invalid_and_costs_no_rpc():
    actor = FakeActor()
    sandbox = make_sandbox(actor, probe=None)
    with pytest.raises(InvalidError):
        sandbox.wait_until_ready()
    assert actor.wait_ready_calls == []


def test_ready_returns_and_passes_the_timeout_through():
    actor = FakeActor("ready")
    make_sandbox(actor).wait_until_ready(timeout=42)
    assert actor.wait_ready_calls == [42]


def test_the_default_timeout_matches_modal():
    actor = FakeActor("ready")
    make_sandbox(actor).wait_until_ready()
    assert actor.wait_ready_calls == [300]


def test_readiness_is_cached_so_a_second_call_costs_no_rpc():
    """Readiness is monotonic, so only the first call goes over the wire."""
    actor = FakeActor("ready")
    sandbox = make_sandbox(actor)
    sandbox.wait_until_ready()
    sandbox.wait_until_ready()
    assert len(actor.wait_ready_calls) == 1


def test_timeout_raises_modals_timeout_error():
    with pytest.raises(TimeoutError):
        make_sandbox(FakeActor("timeout")).wait_until_ready(timeout=1)


def test_a_probe_timeout_is_not_a_sandbox_timeout():
    """Modal's hierarchy puts both under TimeoutError, but a probe that did not
    pass says nothing about the Sandbox having exceeded its own lifetime."""
    with pytest.raises(TimeoutError) as excinfo:
        make_sandbox(FakeActor("timeout")).wait_until_ready(timeout=1)
    assert not isinstance(excinfo.value, SandboxTimeoutError)


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


def test_timeout_leaves_the_sandbox_waitable_again():
    """A probe that has not passed yet is not a terminal state: the caller may
    reasonably wait again with a longer timeout."""
    actor = FakeActor("timeout")
    sandbox = make_sandbox(actor)
    with pytest.raises(TimeoutError):
        sandbox.wait_until_ready(timeout=1)
    with pytest.raises(TimeoutError):
        sandbox.wait_until_ready(timeout=2)
    assert actor.wait_ready_calls == [1, 2]


def test_aio_variant_exists():
    actor = FakeActor("ready")
    sandbox = make_sandbox(actor)
    asyncio.run(sandbox.wait_until_ready.aio(timeout=7))
    assert actor.wait_ready_calls == [7]


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


def test_the_main_process_is_not_counted_as_an_attempt():
    """The probe is a separate exec session; it never inspects or waits on the
    long-running main process it is checking."""
    actor = make_actor(returncodes=(0,))

    async def scenario():
        main_exec_id = await actor.launch_main(MAIN_COMMAND)
        await actor.wait_ready(5)
        return main_exec_id

    main_exec_id = asyncio.run(scenario())
    assert main_exec_id == "exec-1"
    assert main_exec_id not in actor.released


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


def test_a_probe_that_never_passes_times_out():
    actor = make_actor(returncodes=(1,))

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        return await actor.wait_ready(0.05)

    assert asyncio.run(scenario())["outcome"] == "timeout"


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
    # Later callers get the same answer at once rather than waiting.
    assert again["outcome"] == "timeout"
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


def test_a_probe_that_cannot_run_ends_rather_than_hanging():
    """A command missing from the image must not leave a waiter blocked until
    its own timeout with nothing logged."""

    async def boom(exec_id, argv, **kwargs):
        raise FileNotFoundError("no such command")

    actor = make_actor(returncodes=(1,), exec_start=boom)

    async def scenario():
        await actor.launch_main(MAIN_COMMAND)
        return await actor.wait_ready(5)

    assert asyncio.run(scenario())["outcome"] == "ended"


def test_start_returns_the_image_config_alongside_the_instance_id():
    """Bundled so bringing a sandbox up costs one round trip, not two."""
    assert "image_config" in inspect.getsource(SandboxActor.start)
    signature = inspect.signature(_Sandbox.create)
    assert signature.parameters["readiness_probe"].default is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
