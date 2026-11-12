"""Unit tests for the Modal-compatible process handle.

These drive :class:`_ContainerProcess` against a fake actor, so they need
neither runsc nor a Ray cluster.
"""

import asyncio
import functools
import sys
import time
from typing import List, Optional

import pytest

import ray
from ray.experimental.sandbox.modal.container_process import _ContainerProcess
from ray.experimental.sandbox.modal.exception import InvalidError, NotFoundError


class _RemoteShim:
    """Gives a plain coroutine the ``.remote()`` call shape of an actor method."""

    def __init__(self, fn):
        self._fn = fn

    def options(self, **kwargs):
        return self

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class FakeActor:
    """Stands in for ``_SandboxActor`` for the poll/wait/kill calls.

    ``returncode=None`` models a command that is still running, which is what
    a timed-out one looks like from the actor's side until it is killed.
    """

    def __init__(self, returncode: Optional[int] = None):
        self._returncode = returncode
        # Every call made, in order, so a test can assert the actor was not
        # consulted at all.
        self.calls: List[str] = []

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _RemoteShim(functools.partial(impl, self))

    async def _exec_poll(self, exec_id):
        self.calls.append("poll")
        return self._returncode

    async def _exec_wait(self, exec_id, timeout):
        self.calls.append("wait")
        return self._returncode

    async def _exec_kill(self, exec_id):
        self.calls.append("kill")
        self._returncode = -1


def make_process(actor, **kwargs) -> _ContainerProcess:
    return _ContainerProcess(actor, "exec-1", **kwargs)


def run(coro):
    return asyncio.run(coro)


# -- poll and wait ---------------------------------------------------------


def test_poll_asks_the_actor_until_the_process_ends_then_keeps_the_code():
    actor = FakeActor()
    process = make_process(actor, exec_deadline=time.monotonic() + 60)

    assert run(process.poll()) is None
    actor._returncode = 7
    assert run(process.poll()) == 7
    actor._returncode = 99
    assert run(process.poll()) == 7
    assert actor.calls == ["poll", "poll"]


# A command that is still running (None) and one that exited cleanly before
# the deadline (0). Past the deadline, Modal reports -1 for both: its client
# raises ExecTimeoutError before any RPC once the deadline is behind it.
@pytest.mark.parametrize("returncode", [None, 0])
@pytest.mark.parametrize("method", ["poll", "wait"])
def test_past_the_deadline_reports_minus_one_without_asking(method, returncode):
    """Modal's ContainerProcess reports a timeout as -1 rather than raising --
    which is also what lets a ported `while process.poll() is None:` loop end."""
    actor = FakeActor(returncode)
    process = make_process(actor, exec_deadline=time.monotonic() - 1)

    assert run(getattr(process, method)()) == -1
    assert actor.calls == [], "Modal's client asks nobody past the deadline"


def test_a_code_seen_before_the_deadline_outlives_it():
    """Only the first observation is at stake: a cached code is kept."""
    actor = FakeActor(returncode=0)
    process = make_process(actor, exec_deadline=time.monotonic() + 60)
    assert run(process.poll()) == 0

    process._exec_deadline = time.monotonic() - 1
    assert run(process.poll()) == 0
    assert run(process.wait()) == 0


def test_a_deadline_passing_during_wait_reports_minus_one_without_killing():
    """The actor stops the command on its own clock, as Modal's worker does,
    so the handle only reports it."""
    actor = FakeActor()  # exec_wait returns None: the deadline passed first
    process = make_process(actor, exec_deadline=time.monotonic() + 60)

    assert run(process.wait()) == -1
    assert actor.calls == ["wait"]


def test_the_streams_share_the_exec_deadline():
    """On Modal an exec's output is readable only until its timeout."""
    deadline = time.monotonic() + 30
    process = make_process(FakeActor(), exec_deadline=deadline)
    assert process._stdout._deadline == deadline
    assert process._stderr._deadline == deadline


class _GoneActor:
    """An actor that has gone away: every call fails the way Ray reports it."""

    def __getattr__(self, name):
        return _RemoteShim(self._gone)

    async def _gone(self, *args, **kwargs):
        raise ray.exceptions.RayActorError()


@pytest.mark.parametrize("method", ["poll", "wait"])
def test_a_process_whose_sandbox_is_gone_reports_not_found(method):
    """Modal answers NotFoundError here; a raw RayActorError used to escape."""
    with pytest.raises(NotFoundError, match="unavailable"):
        run(getattr(make_process(_GoneActor()), method)())


# -- returncode ------------------------------------------------------------


def test_returncode_is_refused_until_wait():
    """Same exception and same message as Modal's."""
    process = make_process(FakeActor(returncode=5))
    with pytest.raises(InvalidError, match="must call wait"):
        _ = process.returncode
    run(process.wait())
    assert process.returncode == 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
