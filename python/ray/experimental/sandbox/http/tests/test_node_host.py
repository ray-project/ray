import asyncio
import inspect
import sys
from types import SimpleNamespace

import pytest

from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.node_host import SandboxNodeHost
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
    host = SandboxNodeHost(_Sandbox)
    host._executor_set = True  # keep the test loop's default executor
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
