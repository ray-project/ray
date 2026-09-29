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
from ray.experimental.sandbox.modal.container_process import (
    ContainerProcess,
    _ContainerProcess,
)
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


# -- poll ------------------------------------------------------------------


def test_poll_reports_none_while_the_process_runs():
    assert run(make_process(FakeActor()).poll()) is None


def test_poll_reports_the_exit_code_once_the_process_ends():
    assert run(make_process(FakeActor(returncode=3)).poll()) == 3


# A command that is still running (None) and one that exited cleanly before
# the deadline (0). Past the deadline, Modal reports -1 for both: its client
# raises ExecTimeoutError before any RPC once the deadline is behind it.
_EITHER_STATE = pytest.mark.parametrize("returncode", [None, 0])


@_EITHER_STATE
def test_a_poll_past_the_deadline_reports_minus_one_without_asking(returncode):
    """Also what lets a ported `while process.poll() is None:` loop end."""
    actor = FakeActor(returncode)
    process = make_process(actor, exec_deadline=time.monotonic() - 1)

    assert run(process.poll()) == -1
    assert actor.calls == [], "Modal's client asks nobody past the deadline"


def test_poll_leaves_a_process_alone_before_its_deadline():
    actor = FakeActor()
    process = make_process(actor, exec_deadline=time.monotonic() + 60)

    assert run(process.poll()) is None
    assert actor.calls == ["poll"]


def test_a_code_seen_before_the_deadline_outlives_it():
    """Only the first observation is at stake: a cached code is kept."""
    actor = FakeActor(returncode=0)
    process = make_process(actor, exec_deadline=time.monotonic() + 60)
    assert run(process.poll()) == 0

    process._exec_deadline = time.monotonic() - 1
    assert run(process.poll()) == 0
    assert run(process.wait()) == 0


def test_poll_is_answered_from_the_cache_once_it_has_an_exit_code():
    actor = FakeActor(returncode=7)
    process = make_process(actor)

    assert run(process.poll()) == 7
    actor._returncode = 99
    assert run(process.poll()) == 7


# -- wait ------------------------------------------------------------------


def test_wait_returns_the_exit_code():
    assert run(make_process(FakeActor(returncode=2)).wait()) == 2


@_EITHER_STATE
def test_a_wait_past_the_deadline_reports_minus_one_without_asking(returncode):
    """Modal's ContainerProcess reports a timeout as -1 rather than raising."""
    actor = FakeActor(returncode)
    process = make_process(actor, exec_deadline=time.monotonic() - 1)

    assert run(process.wait()) == -1
    assert actor.calls == []


def test_a_deadline_passing_during_wait_reports_minus_one_without_killing():
    """The actor stops the command on its own clock, as Modal's worker does,
    so the handle only reports it."""
    actor = FakeActor()  # exec_wait returns None: the deadline passed first
    process = make_process(actor, exec_deadline=time.monotonic() + 60)

    assert run(process.wait()) == -1
    assert actor.calls == ["wait"]


def test_poll_and_wait_agree_after_a_timeout():
    actor = FakeActor()
    process = make_process(actor, exec_deadline=time.monotonic() - 1)

    assert run(process.poll()) == -1
    assert run(process.wait()) == -1


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


def test_returncode_before_wait_is_refused():
    """Same exception and same message as Modal's."""
    with pytest.raises(InvalidError, match="must call wait"):
        _ = make_process(FakeActor()).returncode


def test_returncode_is_available_after_wait():
    process = make_process(FakeActor(returncode=5))
    run(process.wait())
    assert process.returncode == 5


# -- public surface --------------------------------------------------------


def test_the_public_process_accepts_modals_generic_subscript():
    """Modal types this ContainerProcess[str] / ContainerProcess[bytes]."""
    assert ContainerProcess[str] is ContainerProcess
    assert ContainerProcess[bytes] is ContainerProcess


@pytest.mark.parametrize("name", ["poll", "wait"])
def test_public_methods_carry_an_aio_variant(name):
    process = ContainerProcess._from_impl(make_process(FakeActor(returncode=0)))
    assert hasattr(getattr(process, name), "aio")


def test_the_public_process_has_no_constructor():
    with pytest.raises(InvalidError, match="no public constructor"):
        ContainerProcess()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
