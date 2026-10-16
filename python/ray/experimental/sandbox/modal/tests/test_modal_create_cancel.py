"""A cancelled ``Sandbox.create`` stops its sandbox's start and cleans up.

The actor half drives the real ``_SandboxActor`` implementation class with a
stand-in runtime, and the client half stands in for the actor, so neither
needs runsc or a Ray cluster.
"""

import asyncio
import threading
import types

import pytest

from ray.experimental.sandbox.exceptions import SandboxImageBuildError
from ray.experimental.sandbox.modal import _actor as actor_module, sandbox as client
from ray.experimental.sandbox.modal._actor import _SandboxActor

# The plain class behind the @ray.remote decorator, so it can be driven
# directly without a cluster.
_ActorImpl = _SandboxActor.__ray_metadata__.modified_class

_INSTANCE = "ray-sandbox-000000000001"


class FakeRuntime:
    """Its create blocks, as a build does, until ``cancel_builds``.

    Args:
        finish: Whether the create then succeeds anyway -- it was past its
            build when the cancel came -- rather than fail as a cancelled
            build does.
    """

    def __init__(self, finish: bool):
        self.finish = finish
        self.cancels = 0
        self.deleted = []
        self._release = threading.Event()

    def create(self, **kwargs):
        assert self._release.wait(30)
        if self.finish:
            return _INSTANCE
        raise SandboxImageBuildError("The image build was cancelled.")

    def cancel_builds(self):
        self.cancels += 1
        self._release.set()

    def delete(self, instance_id):
        self.deleted.append(instance_id)


@pytest.mark.parametrize("finish", [False, True])
def test_a_cancelled_start_stops_its_create_and_deletes_what_it_made(
    monkeypatch, finish
):
    runtime = FakeRuntime(finish)
    monkeypatch.setattr(actor_module, "SandboxRuntime", lambda: runtime)
    actor = _ActorImpl.__new__(_ActorImpl)
    _ActorImpl.__init__(actor, {"image": "busybox"}, None)

    async def main():
        start = asyncio.ensure_future(actor.start())
        await asyncio.sleep(0.2)
        start.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start

    asyncio.run(main())
    assert runtime.cancels >= 1
    # A sandbox the create made regardless is deleted: nobody else knows it.
    assert runtime.deleted == ([_INSTANCE] if finish else [])


def _fake_ray(monkeypatch):
    calls = []
    monkeypatch.setattr(client.ray, "cancel", lambda ref: calls.append("cancel"))
    monkeypatch.setattr(client.ray, "kill", lambda actor: calls.append("kill"))
    return calls


def _fake_actor(calls):
    async def terminate():
        calls.append("terminate")

    return types.SimpleNamespace(
        terminate=types.SimpleNamespace(remote=lambda: terminate())
    )


@pytest.mark.parametrize("outcome", ["cancelled", "started"])
def test_a_cancelled_create_waits_for_the_start_then_kills_the_actor(
    monkeypatch, outcome
):
    calls = _fake_ray(monkeypatch)

    async def main():
        start_ref = asyncio.get_running_loop().create_future()
        abandon = asyncio.ensure_future(
            client._abandon_start(_fake_actor(calls), start_ref)
        )
        await asyncio.sleep(0.05)
        # Not killed while the start is still cleaning up.
        assert calls == ["cancel"]
        if outcome == "cancelled":
            start_ref.set_exception(RuntimeError("TaskCancelledError"))
        else:
            # The start finished before the cancel reached it.
            start_ref.set_result({"instance_id": _INSTANCE})
        await abandon

    asyncio.run(main())
    expected = ["cancel", "terminate", "kill"] if outcome == "started" else None
    assert calls == (expected or ["cancel", "kill"])


def test_a_second_cancel_stops_the_wait_but_still_kills_the_actor(monkeypatch):
    calls = _fake_ray(monkeypatch)

    async def main():
        start_ref = asyncio.get_running_loop().create_future()
        abandon = asyncio.ensure_future(
            client._abandon_start(_fake_actor(calls), start_ref)
        )
        await asyncio.sleep(0.05)
        abandon.cancel()
        with pytest.raises(asyncio.CancelledError):
            await abandon

    asyncio.run(main())
    assert calls == ["cancel", "kill"]
