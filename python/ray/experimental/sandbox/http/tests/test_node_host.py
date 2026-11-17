import asyncio
import inspect
import sys
from types import SimpleNamespace

import pytest

from ray.experimental.sandbox.http import node_host, resolver as resolver_module
from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.node_host import SandboxNodeHost, _Slab
from ray.experimental.sandbox.http.resolver import (
    NODE_HOST_PREFIX,
    RayActorHandleResolver,
    _hosted_sandbox_id,
)
from ray.experimental.sandbox.http.schemas import SandboxAPISettings


class _Sandbox:
    """A hosted sandbox that boots at once."""

    def __init__(self, *args, on_exit=None, **kwargs) -> None:
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
    host._node_total = {}
    host._floating = {}
    host._float_task = None
    host._float_blocked_until = 0.0
    return host


def test_network_access_updates_reach_the_sandbox():
    class _Networked(_Sandbox):
        async def set_network_access(self, cidr_allowlist):
            self.cidr_allowlist = cidr_allowlist
            return {"cidr_allowlist": cidr_allowlist}

    async def scenario():
        host = _host()
        sandbox = host._sandboxes["sb-here"] = _Networked()
        updated = await host.set_network_access("sb-here", ["1.1.1.1/32"])
        missing = await host.set_network_access("sb-gone", ["1.1.1.1/32"])
        return sandbox.cidr_allowlist, updated, missing

    applied, updated, missing = asyncio.run(scenario())
    assert applied == updated["cidr_allowlist"] == ["1.1.1.1/32"]
    assert missing["error_code"] == "sandbox_not_found"


def test_the_node_host_has_every_method_the_api_calls_on_a_sandbox():
    """A hosted handle calls the node host's method of the same name, so each
    ``SandboxHost`` method the API layers call needs one there."""
    # The node host calls these on its sandboxes itself.
    own = {"finish", "to_dict"}
    called = {
        name
        for name, _ in inspect.getmembers(SandboxHost, inspect.isfunction)
        if not name.startswith("_")
    } - own
    assert sorted(n for n in called if not hasattr(SandboxNodeHost, n)) == []


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


def test_a_full_node_grows_no_slab():
    """Ray omits a resource with nothing left from a node's available ones;
    the host reads that as none free rather than as unknown, and reserves
    nothing (a slab there could never be placed)."""

    async def scenario():
        host = _host()
        host._slab_shape = {"CPU": 4.0}
        host._node_free = lambda: {"memory": 1e9}  # CPU fully reserved
        grown = []

        async def grow_one(shape, wait_seconds=None):
            grown.append(shape)
            return None

        host._grow_one = grow_one
        demand = {"CPU": 0.25}
        host._waiting.append(demand)
        return await host._grow(demand), grown

    assert asyncio.run(scenario()) == (None, [])


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


def test_a_host_that_knows_its_node_admits_before_its_slab_is_reserved():
    """With the node's resources known, an admit doesn't wait for Ray to
    place a slab: the sandbox is added at once, and takes its share of the
    slab once the reservation is there."""

    async def scenario():
        host = _host()
        host._slab_shape = {"CPU": 1.0}
        host._node_total = {"CPU": 2.0}
        placed = asyncio.Event()
        shapes = []

        async def grow_one(shape, wait_seconds=None):
            shapes.append(dict(shape))
            await placed.wait()
            return host._add_slab(f"pg-{len(shapes)}", shape)

        async def grow(demand):
            return None  # the node has no room left for another slab

        host._grow_one = grow_one
        host._grow = grow
        spec = {"image": "python:3.12-slim"}
        demand = {"CPU": 0.5}
        early = [await host.admit(f"sb-{i}", spec, {}, demand) for i in range(4)]
        floating = dict(host._floating)
        # A fifth would put the node past its two CPUs.
        over = await host.admit("sb-4", spec, {}, demand)
        placed.set()
        await host._float_task
        return early, floating, over, shapes, host

    early, floating, over, shapes, host = asyncio.run(scenario())
    assert early == [{"ok": True}] * 4
    assert sorted(floating) == ["sb-0", "sb-1", "sb-2", "sb-3"]
    assert over["error_code"] == "full"
    # One group covers every sandbox admitted while it was being reserved.
    assert shapes == [{"CPU": 2.0}]
    assert host._floating == {}
    assert sorted(host._slab_of) == ["sb-0", "sb-1", "sb-2", "sb-3"]
    assert [slab.used for slab in host._slabs] == [{"CPU": 2.0}]


def test_a_slab_ray_cannot_place_stops_admits_ahead_of_reservations():
    """If Ray can't place the slab on this node, something else holds the
    node's resources: the host stops admitting ahead of its reservations and
    answers "full", and the sandboxes it already admitted stay."""

    async def scenario():
        host = _host()
        host._slab_shape = {"CPU": 1.0}
        host._node_total = {"CPU": 4.0}

        async def grow_one(shape, wait_seconds=None):
            return None

        async def grow(demand):
            return None

        host._grow_one = grow_one
        host._grow = grow
        spec = {"image": "python:3.12-slim"}
        first = await host.admit("sb-0", spec, {}, {"CPU": 0.5})
        await host._float_task
        second = await host.admit("sb-1", spec, {}, {"CPU": 0.5})
        released = await host.remove("sb-0")
        return first, second, list(host._sandboxes), dict(host._floating), released

    first, second, sandboxes, floating, _ = asyncio.run(scenario())
    assert first == {"ok": True}
    assert second["error_code"] == "full"
    assert sandboxes == [] and floating == {}


class _FakeHost:
    """A node host handle whose methods run a function in place of the actor."""

    def __init__(self, node_id, **methods):
        self._actor_id = SimpleNamespace(hex=lambda: node_id)
        self.calls = []
        for name, fn in methods.items():
            setattr(self, name, SimpleNamespace(remote=self._remote(name, fn)))

    def _remote(self, name, fn):
        def remote(*args, **kwargs):
            self.calls.append(name)

            async def run():
                return fn(*args, **kwargs)

            return run()

        return remote


def _node_resolver(monkeypatch, nodes, **settings):
    """A node-mode resolver over fake nodes, with no Ray behind it."""
    import ray

    resolver = RayActorHandleResolver(SandboxAPISettings(host_mode="node", **settings))
    monkeypatch.setattr(
        ray,
        "nodes",
        lambda: [
            {"NodeID": n, "Alive": True, "Resources": {"CPU": 8.0}} for n in nodes
        ],
    )
    return resolver


def test_hosted_ids_resolve_without_scanning_the_nodes(monkeypatch):
    """A hosted sandbox id names its node by a prefix, which an index maps
    to the node; a prefix two nodes share names neither."""
    nodes = ["aaaaaaaaaa" + "1" * 46, "bbbbbbbbbb" + "2" * 46, "bbbbbbbbbb" + "3" * 46]
    resolver = _node_resolver(monkeypatch, nodes)
    resolver._cpu_nodes(refresh=True)
    resolver._node_hosts[nodes[0]] = ("host-a", object())
    sandbox_id = _hosted_sandbox_id(nodes[0])
    handle = resolver.cached(sandbox_id)
    assert handle.host_name == "host-a" and handle.sandbox_id == sandbox_id
    assert resolver.cached(_hosted_sandbox_id(nodes[1])) is None
    assert resolver._node_for_prefix("bbbbbbbbbb") is None
    assert resolver._node_for_prefix("aaaaaaaaaa") == nodes[0]


def test_admit_skips_full_hosts_and_stops_after_its_probes(monkeypatch):
    """A host that answers "full" isn't asked again for a moment, and one
    create asks a bounded number of hosts, so a create on a nearly full
    cluster doesn't ask every host in turn."""
    monkeypatch.setattr(resolver_module, "_MAX_ADMIT_PROBES", 3)
    nodes = [f"{i:010d}" + "0" * 46 for i in range(8)]
    resolver = _node_resolver(monkeypatch, nodes)
    full = {"error_code": "full", "message": "full"}
    hosts = {}
    for i, node in enumerate(nodes):
        answer = {"ok": True} if i == 7 else full
        hosts[node] = _FakeHost(node, admit=lambda *a, _answer=answer: _answer)
        resolver._node_hosts[node] = (f"host-{i}", hosts[node])
    ctor = {"spec": {"image": "img"}, "settings": {}}

    async def scenario():
        resolver._cpu_nodes(refresh=True)
        results = []
        for _ in range(4):
            results.append(await resolver._admit({"CPU": 0.25}, ctor))
        return results

    results = asyncio.run(scenario())
    asked = sorted(i for i, node in enumerate(nodes) if hosts[node].calls)
    # Two creates stop at their probe limit and remember the full hosts, so
    # the third asks only hosts nobody asked yet and reaches the one with
    # room: nine calls, where asking every host in turn would take 22.
    assert [r is not None for r in results] == [False, False, True, True]
    assert sum(len(h.calls) for h in hosts.values()) == 3 + 3 + 2 + 1
    assert asked == list(range(8))


def test_prestart_starts_every_host_before_waiting_for_any(monkeypatch):
    import ray

    nodes = [f"{i:010d}" + "0" * 46 for i in range(5)]
    resolver = _node_resolver(monkeypatch, nodes)
    order = []

    def node_host(node_id):
        order.append(("start", node_id))
        return (
            "host",
            SimpleNamespace(sandbox_ids=SimpleNamespace(remote=lambda: node_id)),
        )

    def wait(refs, num_returns, timeout):
        order.append(("wait", len(refs)))
        return refs, []

    monkeypatch.setattr(resolver, "_node_host", node_host)
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", lambda ref, timeout=None: [])
    assert resolver.prestart() == 5
    assert order[:5] == [("start", n) for n in nodes]
    assert order[5] == ("wait", 5)


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
