import asyncio
import sys
from types import SimpleNamespace

import pytest

from ray.experimental.sandbox.http.node_host import SandboxNodeHost
from ray.experimental.sandbox.http.resolver import (
    NODE_HOST_PREFIX,
    RayActorHandleResolver,
)
from ray.experimental.sandbox.http.schemas import SandboxAPISettings


class _Sandbox:
    """A hosted sandbox that boots at once."""

    def __init__(self, sandbox_id, spec, settings, on_exit=None) -> None:
        self.on_exit = on_exit
        self.booted = False
        self.shut_down = False

    async def boot(self) -> None:
        self.booted = True

    async def _shutdown(self) -> None:
        self.shut_down = True


def _host() -> SandboxNodeHost:
    host = SandboxNodeHost(_Sandbox)
    host._executor_set = True  # keep the test loop's default executor
    return host


def test_add_boots_once_and_remove_releases_the_reservation():
    async def scenario():
        host = _host()
        released = []

        async def remove_group(reservation, what):
            released.append(reservation)

        host._remove_group = remove_group
        for _ in range(2):  # a repeated add (a lost reply) is a no-op
            assert await host.add("sb-1", {"image": "img"}, {}, "pg-1") == {"ok": True}
        sandbox = host._sandboxes["sb-1"]
        await asyncio.sleep(0)
        await host.remove("sb-1")
        await asyncio.sleep(0)
        return sandbox, released, await host.sandbox_ids()

    sandbox, released, left = asyncio.run(scenario())
    assert sandbox.booted and sandbox.shut_down
    assert released == ["pg-1"]
    assert left == []


def test_a_sandbox_that_exits_on_its_own_frees_its_reservation():
    """A terminate or TTL ends in the sandbox's on_exit, which must forget it
    and release its placement group, as killing its own actor would."""

    async def scenario():
        host = _host()
        released = []

        async def remove_group(reservation, what):
            released.append(reservation)

        host._remove_group = remove_group
        await host.add("sb-1", {"image": "img"}, {}, "pg-1")
        host._sandboxes["sb-1"].on_exit()
        await asyncio.sleep(0)
        return released, await host.describe("sb-1")

    released, described = asyncio.run(scenario())
    assert released == ["pg-1"]
    assert described["status"] == "terminated"
    assert described["error_code"] == "sandbox_not_found"


def test_background_tasks_are_held_until_done():
    async def scenario():
        host = _host()
        release = asyncio.Event()
        host._spawn(release.wait())
        assert len(host._background) == 1
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return len(host._background)

    assert asyncio.run(scenario()) == 0


def test_list_names_skips_a_failing_host(monkeypatch):
    import ray
    import ray.util

    resolver = RayActorHandleResolver(SandboxAPISettings(host_mode="node"))
    namespace = resolver._settings.namespace
    monkeypatch.setattr(
        ray.util,
        "list_named_actors",
        lambda all_namespaces: [
            {"namespace": namespace, "name": NODE_HOST_PREFIX + "a"},
            {"namespace": namespace, "name": NODE_HOST_PREFIX + "b"},
            {"namespace": namespace, "name": "sb-named"},
        ],
    )

    class Host:
        def __init__(self, name):
            self.sandbox_ids = SimpleNamespace(remote=lambda: name)

    monkeypatch.setattr(ray, "get_actor", lambda name, namespace: Host(name))
    monkeypatch.setattr(ray, "wait", lambda refs, num_returns, timeout: (refs, []))

    def get(ref):
        if ref.endswith("b"):
            raise RuntimeError("host b died")
        return ["sb-on-a"]

    monkeypatch.setattr(ray, "get", get)
    assert sorted(resolver.list_names()) == ["sb-named", "sb-on-a"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
