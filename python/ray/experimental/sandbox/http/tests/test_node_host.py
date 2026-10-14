import asyncio
import sys
from types import SimpleNamespace

import pytest

from ray.experimental.sandbox.http import node_host
from ray.experimental.sandbox.http.node_host import SandboxNodeHost, _Slab
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
    # __init__ needs a Ray worker (the host's node id).
    host = object.__new__(SandboxNodeHost)
    host._host_class = _Sandbox
    host._sandboxes = {}
    host._reservations = {}
    host._executor_set = True  # keep the test loop's default executor
    host._node_id = "node"
    host._slab_shape = {}
    host._slabs = []
    host._slab_of = {}
    host._grow_lock = asyncio.Lock()
    host._waiting = []
    host._full_until = 0.0
    host._background = set()
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


def test_a_slab_no_admit_takes_is_released_when_idle(monkeypatch):
    """A slab grown for an admit that ended up not taking it (a repeat whose
    sandbox was already added) is released after the idle time."""
    monkeypatch.setattr(node_host, "_SLAB_IDLE_SECONDS", 0.05)

    async def scenario():
        host = _host()
        removed = []

        async def remove_group(reservation, what):
            removed.append(reservation)

        host._remove_group = remove_group
        unused = host._add_slab("pg-unused", {"CPU": 1.0})
        used = host._add_slab("pg-used", {"CPU": 1.0})
        used.take("sb-1", {"CPU": 0.5})
        await asyncio.sleep(0.2)
        return removed, [slab.reservation for slab in host._slabs], unused

    removed, left, unused = asyncio.run(scenario())
    assert removed == ["pg-unused"]
    assert left == ["pg-used"]


def test_a_repeated_admit_takes_its_share_once():
    """A repeat of an admit (a lost reply replayed through Ray) that arrives
    while the first waits must not charge the slab twice."""

    async def scenario():
        host = _host()
        slab = _Slab(
            SimpleNamespace(id=SimpleNamespace(hex=lambda: "pg")), {"CPU": 1.0}
        )

        async def grow(demand):
            # Both admits wait here, for the one slab this grows.
            await asyncio.sleep(0.05)
            if slab not in host._slabs:
                host._slabs.append(slab)
            return slab

        host._grow = grow
        spec = {"image": "python:3.12-slim"}
        demand = {"CPU": 0.25}
        results = await asyncio.gather(
            *(host.admit("sb-1", spec, {}, demand) for _ in range(2))
        )
        return results, slab.used, list(host._sandboxes), list(slab.sandboxes)

    results, used, sandboxes, holders = asyncio.run(scenario())
    assert results == [{"ok": True}, {"ok": True}]
    assert used == {"CPU": 0.25}
    assert sandboxes == holders == ["sb-1"]


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
