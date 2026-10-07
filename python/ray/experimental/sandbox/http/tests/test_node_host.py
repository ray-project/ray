import asyncio
import collections
import sys
import time
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
    """Counts how many boots run at once."""

    active = 0
    peak = 0

    def __init__(self, *args, **kwargs) -> None:
        self._status = "running"
        self._error = None
        self._terminating = False
        self.shut_down = False
        self.alive = True

    def container_running(self) -> bool:
        return self.alive

    async def boot(self) -> None:
        _Sandbox.active += 1
        _Sandbox.peak = max(_Sandbox.peak, _Sandbox.active)
        await asyncio.sleep(0.02)
        _Sandbox.active -= 1

    async def _shutdown(self) -> None:
        self.shut_down = True


def _host() -> SandboxNodeHost:
    # The warm-pool parts only; __init__ needs a Ray worker.
    host = object.__new__(SandboxNodeHost)
    host._host_class = _Sandbox
    host._exec_starts = collections.deque()
    host._fill_slots = asyncio.Semaphore(node_host._WARM_FILL_CONCURRENCY)
    host._busy_slot = asyncio.Semaphore(1)
    host._grow_lock = asyncio.Lock()
    host._warm = {"key": collections.deque()}
    host._warm_booting = {"key": 0}
    host._warm_failed_at = {}
    host._warm_settings = {}
    host._warm_templates = {}
    host._warm_reserve = {}
    host._background = set()
    return host


@pytest.mark.parametrize("busy,expected_peak", [(False, 4), (True, 1)])
def test_refill_boots_one_at_a_time_while_execs_are_busy(busy, expected_peak):
    async def scenario():
        host = _host()
        if busy:
            now = time.monotonic()
            host._exec_starts.extend([now] * (node_host._BUSY_EXECS_PER_SECOND + 1))
        _Sandbox.peak = 0
        host._warm_templates = {"key": ({}, 8)}
        host._warm_booting["key"] = 8
        await asyncio.gather(*(host._boot_warm("key", {}) for _ in range(8)))
        return _Sandbox.peak, len(host._warm["key"]), host._warm_booting["key"]

    assert asyncio.run(scenario()) == (expected_peak, 8, 0)


class _FailingSandbox(_Sandbox):
    """A warm sandbox whose boot fails, as with an image that can't be pulled."""

    boots = 0

    async def boot(self) -> None:
        _FailingSandbox.boots += 1
        self._status = "error"
        self._error = "image pull failed"


def test_a_failed_warm_boot_backs_off():
    """After a warm boot fails, refills boot no replacements until the retry
    interval has passed, rather than a full batch on every create."""

    async def scenario():
        host = _host()
        host._host_class = _FailingSandbox
        host._warm_templates = {"key": ({}, 2)}
        _FailingSandbox.boots = 0
        host._refill()
        await asyncio.gather(*host._background)
        failed_boots = _FailingSandbox.boots
        for _ in range(5):
            host._refill()
        await asyncio.sleep(0)
        during_backoff = _FailingSandbox.boots
        host._warm_failed_at["key"] -= node_host._WARM_RETRY_SECONDS
        host._refill()
        await asyncio.gather(*host._background)
        return failed_boots, during_backoff, _FailingSandbox.boots

    assert asyncio.run(scenario()) == (2, 2, 4)


def test_exec_starts_older_than_a_second_do_not_count():
    host = _host()
    now = time.monotonic()
    host._exec_starts.extend([now - 5] * 500 + [now] * 3)
    assert host._recent_exec_starts(now) == 3
    assert len(host._exec_starts) == 3
    assert not host._busy()


@pytest.mark.parametrize("slab,reserved", [(None, False), ({"CPU": 4.0}, True)])
def test_warm_pool_reserve_needs_slabs(monkeypatch, slab, reserved):
    """Only creates placed in slabs use a pool's reserve: a host without slabs
    must not hold one beside each create's own placement group."""
    import ray

    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "n")
    )
    spec = {"image": "python:3.12-slim", "network": "none"}
    host = SandboxNodeHost(
        _Sandbox, slab, [{"spec": spec, "size": 4, "reserve": {"CPU": 1.0}}]
    )
    assert bool(host._warm_reserve) is reserved
    assert host._reserve_floor == ({"CPU": 1.0} if reserved else {})


def test_a_cold_create_starts_filling_the_warm_pool():
    """A host whose pool is empty (its node joined after prestart, or its fill
    failed) starts filling it on the next create, though that create can't
    use the pool."""

    async def scenario():
        host = _host()
        host._sandboxes = {}
        host._reservations = {}
        host._executor_set = True
        host._warm_templates = {"key": ({}, 2)}
        await host.add("sb-cold", {"image": "img", "workdir": "/w"}, {})
        for _ in range(100):
            if len(host._warm["key"]) == 2:
                break
            await asyncio.sleep(0.01)
        return len(host._warm["key"])

    assert asyncio.run(scenario()) == 2


def test_idle_slabs_never_drop_below_the_warm_pool_floor():
    """Two idle timers that started while extra slabs were held: only the
    first drop happens, so the slabs stay at the warm pools' floor."""

    async def scenario():
        host = _host()
        removed = []

        async def remove_group(reservation, what):
            removed.append(reservation)

        host._remove_group = remove_group
        slabs = [_Slab(f"pg{i}", {"CPU": 10.0}) for i in range(4)]
        host._slabs = list(slabs)
        host._reserve_floor = {"CPU": 30.0}
        host._drop_slab(slabs[0])
        host._drop_slab(slabs[1])
        await asyncio.sleep(0)
        return removed, len(host._slabs)

    assert asyncio.run(scenario()) == (["pg0"], 3)


def test_pregrow_skips_what_admits_reserved_for_the_pool():
    """The pool's total waits beside the admits, so an admit that grows first
    reserves it too, and the pool then grows nothing more."""

    async def scenario():
        host = _host()
        host._slabs = []
        host._waiting = []
        host._warm_templates = {"key": ({}, 4)}
        host._reserve_floor = {"CPU": 4.0}
        grown = []

        async def grow(demand):
            need = sum(waiting.get("CPU", 0.0) for waiting in host._waiting)
            grown.append(need)
            host._slabs.append(_Slab(f"pg{len(grown)}", {"CPU": need}))
            return host._slabs[-1]

        host._grow = grow
        async with host._grow_lock:  # an admit holds the lock...
            pregrow = asyncio.ensure_future(host._pregrow({"key": {"CPU": 4.0}}))
            await asyncio.sleep(0)  # ...while the pool's total starts waiting,
            host._waiting.append({"CPU": 1.0})
            await host._grow({"CPU": 1.0})  # and grows for both
            host._waiting.remove({"CPU": 1.0})
        await pregrow
        return grown

    assert asyncio.run(scenario()) == [5.0]


def test_configure_applies_a_later_deployments_pools(monkeypatch):
    """A host an earlier deployment started takes the calling facade's
    settings: a pool no longer listed shuts down and a new one fills."""
    import ray

    monkeypatch.setattr(
        ray, "get_runtime_context", lambda: SimpleNamespace(get_node_id=lambda: "n")
    )
    old = {"image": "old:1", "network": "none"}
    new = {"image": "new:1", "network": "none"}

    async def scenario():
        host = SandboxNodeHost(_Sandbox, None, [{"spec": old, "size": 1}])
        stale = _Sandbox()
        host._warm[node_host.warm_key(old)].append(stale)
        await host.configure(None, [{"spec": new, "size": 2}], {})
        for _ in range(100):
            if len(host._warm[node_host.warm_key(new)]) == 2:
                break
            await asyncio.sleep(0.01)
        return (
            list(host._warm_templates),
            stale.shut_down,
            len(host._warm[node_host.warm_key(new)]),
        )

    templates, stale_shut_down, filled = asyncio.run(scenario())
    assert templates == [node_host.warm_key(new)]
    assert stale_shut_down
    assert filled == 2


def test_a_pooled_sandbox_whose_container_died_is_not_adopted():
    async def scenario():
        host = _host()
        dead, live = _Sandbox(), _Sandbox()
        dead.alive = False
        spec = {"image": "img"}
        key = node_host.warm_key(spec)
        host._warm = {key: collections.deque([dead, live])}
        taken = host._take_warm(spec)
        await asyncio.sleep(0)
        return taken is live, dead.shut_down

    assert asyncio.run(scenario()) == (True, True)


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
        host._slabs = []
        host._reserve_floor = {}
        unused = host._add_slab("pg-unused", {"CPU": 1.0})
        used = host._add_slab("pg-used", {"CPU": 1.0})
        used.take("sb-1", {"CPU": 0.5})
        await asyncio.sleep(0.2)
        return removed, [slab.reservation for slab in host._slabs], unused

    removed, left, unused = asyncio.run(scenario())
    assert removed == ["pg-unused"]
    assert left == ["pg-used"]


def test_background_tasks_are_held_until_done():
    async def scenario():
        host = _host()
        host._background = set()
        release = asyncio.Event()
        host._spawn(release.wait())
        assert len(host._background) == 1
        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return len(host._background)

    assert asyncio.run(scenario()) == 0


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

        host._slabs = []
        host._grow = grow
        host._slab_of = {}
        host._sandboxes = {}
        host._reservations = {}
        host._waiting = []
        host._full_until = 0.0
        host._node_id = "node"
        host._executor_set = True
        spec = {"image": "python:3.12-slim", "workdir": "/w"}  # no warm pool
        demand = {"CPU": 0.25}
        results = await asyncio.gather(
            *(host.admit("sb-1", spec, {}, demand) for _ in range(2))
        )
        return results, slab.used, list(host._sandboxes), list(slab.sandboxes)

    results, used, sandboxes, holders = asyncio.run(scenario())
    assert results == [{"ok": True}, {"ok": True}]
    assert used == {"CPU": 0.25}
    assert sandboxes == holders == ["sb-1"]


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
