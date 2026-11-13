"""Warm pools that publish their sandboxes to a WarmPoolStore.

The pools run on a fake backend, so no runsc is needed.
"""

import collections
import os
import pickle
import socket
import sqlite3
import sys
import threading
import time
import uuid

import pytest

import ray
from ray.experimental.sandbox import RayWarmPoolStore, WarmPoolEntry, WarmPoolStore
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.runtime import SandboxRuntime, _default_host, _WarmPool


class _FakeImageManager:
    def pull_image(self, image, timeout_seconds=120.0):
        return f"/tmp/fake-images/{image}"


class _FakeBackend:
    def __init__(self):
        self.lock = threading.Lock()
        self.booted = 0
        self.live = set()
        self.deleted = []

    def create_sandbox(self, config):
        with self.lock:
            self.booted += 1
            instance_id = f"sb-{self.booted:04d}"
            self.live.add(instance_id)
            return instance_id

    def delete_sandbox(self, sandbox_id):
        with self.lock:
            self.deleted.append(sandbox_id)
            self.live.discard(sandbox_id)

    def get_status(self, sandbox_id):
        if sandbox_id in self.live:
            return SandboxStatus.RUNNING
        return SandboxStatus.TERMINATED

    def assign_config(self, sandbox_id, config):
        pass


class _MemoryStore(WarmPoolStore):
    def __init__(self):
        self.lock = threading.Lock()
        self.hosts = {}
        self.publishes = 0

    def publish(self, host, entries):
        with self.lock:
            self.publishes += 1
            if entries:
                self.hosts[host] = list(entries)
            else:
                self.hosts.pop(host, None)

    def ready(self, profile):
        now = time.time()
        with self.lock:
            return [
                entry
                for entries in self.hosts.values()
                for entry in entries
                if entry.profile == profile
                and entry.state == "ready"
                and entry.expires_at > now
            ]

    def states(self, host):
        with self.lock:
            return sorted(
                (e.profile, e.slot, e.state, e.sandbox_id)
                for e in self.hosts.get(host, [])
            )


_OPEN_RUNTIMES = []


@pytest.fixture(autouse=True)
def _close_runtimes():
    """Close every pooled runtime a test opened, so a failed test doesn't
    leave its host name taken for the next. These tests also start outside
    Ray, whatever an earlier test in the session left running."""
    if ray.is_initialized():
        ray.shutdown()
    yield
    while _OPEN_RUNTIMES:
        _OPEN_RUNTIMES.pop().close()


def _pooled_runtime(profiles, store, host="host-a"):
    runtime = SandboxRuntime()
    runtime._image_manager = _FakeImageManager()
    runtime._backend = _FakeBackend()
    runtime._warm = _WarmPool(runtime, profiles, store, host)
    _OPEN_RUNTIMES.append(runtime)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    return runtime


def _wait_until(predicate, timeout_seconds=5.0):
    deadline = time.monotonic() + timeout_seconds
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def test_a_pool_publishes_its_sandboxes_ready_then_claimed_then_gone():
    store = _MemoryStore()
    runtime = _pooled_runtime([{"image": "img", "size": 2}], store)
    assert _wait_until(
        lambda: [s[2] for s in store.states("host-a")] == ["ready", "ready"]
    )
    (first, second) = store.hosts["host-a"]
    assert (first.profile, {first.slot, second.slot}) == ("img", {0, 1})
    assert first.node_id is None  # no Ray here
    assert first.expires_at > time.time()

    taken = runtime.create(image="img", timeout_seconds=5)
    assert _wait_until(
        lambda: sorted(s[2] for s in store.states("host-a")) == ["claimed", "ready"]
    )
    (claimed,) = [e for e in store.hosts["host-a"] if e.state == "claimed"]
    assert claimed.sandbox_id == taken
    assert store.ready("img")[0].sandbox_id != taken

    # Deleting it boots a new sandbox in the same slot, which turns up ready.
    runtime.delete(taken)
    assert _wait_until(
        lambda: [s[2] for s in store.states("host-a")] == ["ready", "ready"]
    )
    refilled = {e.slot: e.sandbox_id for e in store.hosts["host-a"]}
    assert refilled[claimed.slot] not in (taken, None)

    # Closing removes the host.
    runtime.close()
    assert "host-a" not in store.hosts


def test_profiles_that_boot_the_same_image_need_names():
    store = _MemoryStore()
    profiles = [{"image": "img", "size": 1}, {"image": "img", "cpu": 1.0, "size": 1}]
    with pytest.raises(ValueError, match="'name' each"):
        _pooled_runtime(profiles, store)
    # Names only go to a store. A pool that publishes nowhere needs none.
    _pooled_runtime(profiles, None).close()
    runtime = _pooled_runtime(
        [{**profiles[0], "name": "small"}, {**profiles[1], "name": "big"}],
        store,
        host="host-names",
    )
    assert _wait_until(lambda: len(store.states("host-names")) == 2)
    assert {e.profile for e in store.hosts["host-names"]} == {"small", "big"}
    assert len(store.ready("big")) == 1
    runtime.close()


def test_a_pool_publishes_every_heartbeat(monkeypatch):
    monkeypatch.setattr(
        "ray.experimental.sandbox.runtime._PUBLISH_HEARTBEAT_SECONDS", 0.2
    )
    store = _MemoryStore()
    runtime = _pooled_runtime([{"image": "img", "size": 1}], store)
    assert _wait_until(lambda: len(store.states("host-a")) == 1)
    published = store.publishes
    # Nothing changes, and the host publishes anyway, pushing its expiry on.
    assert _wait_until(lambda: store.publishes >= published + 3)
    (entry,) = store.hosts["host-a"]
    assert time.time() < entry.expires_at <= time.time() + 0.6 + 0.1
    runtime.close()


def test_a_failing_store_doesnt_stop_the_pool(monkeypatch):
    from ray.experimental.sandbox import runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_PUBLISH_HEARTBEAT_SECONDS", 0.05)
    warnings = []
    # Ray's loggers don't propagate to the root logger that caplog watches.
    monkeypatch.setattr(
        runtime_module.logger, "warning", lambda msg, *args: warnings.append(msg % args)
    )

    class _DownStore(_MemoryStore):
        def publish(self, host, entries):
            super().publish(host, entries)
            raise ConnectionError("store unreachable")

    store = _DownStore()
    runtime = _pooled_runtime([{"image": "img", "size": 2}], store)
    first = runtime.create(image="img", timeout_seconds=5)
    runtime.delete(first)
    assert runtime.wait_for_warm_pool(timeout_seconds=10)
    # Every heartbeat publishes, and every publish fails.
    assert _wait_until(lambda: store.publishes >= 5)
    runtime.close()
    # Logged once, not once per publish.
    assert len([w for w in warnings if "store unreachable" in w]) == 1


def test_each_publishing_pool_in_a_process_needs_its_own_host():
    store = _MemoryStore()
    runtime = _pooled_runtime([{"image": "img", "size": 1}], store, host="shared")
    with pytest.raises(ValueError, match="warm_pool_host of its own"):
        _pooled_runtime([{"image": "img", "size": 1}], store, host="shared")
    runtime.close()
    # Once the first pool closes, the name is free.
    _pooled_runtime([{"image": "img", "size": 1}], store, host="shared").close()


def test_pools_that_publish_to_different_stores_may_share_a_host_name():
    first, second = _MemoryStore(), _MemoryStore()
    one = _pooled_runtime([{"image": "img", "size": 1}], first, host="shared")
    two = _pooled_runtime([{"image": "img", "size": 1}], second, host="shared")
    assert _wait_until(lambda: first.ready("img") and second.ready("img"))
    one.close()
    two.close()
    # Two RayWarmPoolStores with one name and namespace are one store.
    assert RayWarmPoolStore("a", "ns") == RayWarmPoolStore("a", "ns")
    assert hash(RayWarmPoolStore("a", "ns")) == hash(RayWarmPoolStore("a", "ns"))
    assert RayWarmPoolStore("a", "ns") != RayWarmPoolStore("b", "ns")


def test_a_pool_that_fails_to_start_frees_its_host(monkeypatch):
    """A pool whose constructor fails is never closed by anyone. Its
    publisher must not keep it, and its host name, for the life of the
    process."""
    from ray.experimental.sandbox import runtime as runtime_module

    store = _MemoryStore()

    def broken(self):
        raise RuntimeError("no executor")

    with monkeypatch.context() as failing:
        failing.setattr(_WarmPool, "refill_all", broken)
        with pytest.raises(RuntimeError, match="no executor"):
            _pooled_runtime([{"image": "img", "size": 1}], store, host="shared")
    assert _wait_until(
        lambda: (store, "shared") not in runtime_module._PUBLISHING_HOSTS
    )
    _pooled_runtime([{"image": "img", "size": 1}], store, host="shared").close()


def test_an_actor_without_a_name_needs_a_host_to_publish(monkeypatch):
    """Callers reach a host by the name it publishes, and Ray finds only
    named actors, so an unnamed actor has no default to publish under."""

    class _Context:
        def __init__(self, name):
            self.name = name

        def get_actor_name(self):
            return self.name

        def get_actor_id(self):
            return "ab" * 16

    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray, "get_runtime_context", lambda: _Context(None))
    with pytest.raises(ValueError, match="warm_pool_host"):
        _default_host(published=True)
    # A pool that publishes nowhere still gets a name of its own.
    assert _default_host() == "ab" * 16
    monkeypatch.setattr(ray, "get_runtime_context", lambda: _Context("host-actor"))
    assert _default_host(published=True) == "host-actor"


def test_a_ray_store_that_cant_answer_lists_nothing(monkeypatch):
    """While the store's actor restarts, or its node hangs, a caller gets no
    ready sandboxes and boots cold, rather than an error from its create. A
    publish still fails, and the pool publishes again at its next heartbeat."""
    store = RayWarmPoolStore(name="unreachable")

    def unanswered(method, *args):
        raise ray.exceptions.GetTimeoutError("the directory didn't answer")

    monkeypatch.setattr(store, "_call", unanswered)
    assert store.ready("img") == []
    assert store.entries() == []
    monkeypatch.setattr(store, "_call", RayWarmPoolStore._call.__get__(store))
    monkeypatch.setattr(
        store,
        "_directory",
        lambda: (_ for _ in ()).throw(ray.exceptions.RayActorError()),
    )
    assert store.ready("img") == []
    with pytest.raises(ray.exceptions.RayActorError):
        store.publish("a", [_entry("a", 0)])


def test_a_closed_pool_keeps_its_host_until_its_last_publish_is_done(monkeypatch):
    """A closed pool whose store is slow to take its last publish keeps its
    host until that publish is done, so that the late publish can't remove
    the records of a new pool that publishes as the same host."""
    from ray.experimental.sandbox import runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_PUBLISH_HEARTBEAT_SECONDS", 0.2)
    release = threading.Event()

    class _SlowStore(_MemoryStore):
        slow = False

        def publish(self, host, entries):
            if self.slow:
                release.wait(10)
            super().publish(host, entries)

    store = _SlowStore()
    runtime = _pooled_runtime([{"image": "img", "size": 1}], store, host="shared")
    store.slow = True
    runtime.close()  # Gives up waiting for the last publish.
    with pytest.raises(ValueError, match="still sending its last publish"):
        _pooled_runtime([{"image": "img", "size": 1}], store, host="shared")
    store.slow = False
    release.set()
    assert _wait_until(
        lambda: (store, "shared") not in runtime_module._PUBLISHING_HOSTS
    )
    new = _pooled_runtime([{"image": "img", "size": 1}], store, host="shared")
    assert _wait_until(lambda: len(store.ready("img")) == 1)
    (entry,) = store.ready("img")
    assert entry.sandbox_id in new._warm._held


def test_a_caller_that_names_listed_sandboxes_gets_exactly_those():
    """Creates that each name a ready record's sandbox take exactly the
    listed sandboxes, in any order, so a caller that sends one create per
    record never sends a host more creates than it has sandboxes ready."""
    store = _MemoryStore()
    runtimes = {
        "host-a": _pooled_runtime([{"image": "img", "size": 3}], store, host="host-a"),
        "host-b": _pooled_runtime([{"image": "img", "size": 1}], store, host="host-b"),
    }
    assert _wait_until(lambda: len(store.ready("img")) == 4)
    entries = sorted(store.ready("img"), key=lambda e: e.slot, reverse=True)
    taken = [
        runtimes[e.host].create(
            image="img", timeout_seconds=5, warm_sandbox=e.sandbox_id
        )
        for e in entries
    ]
    assert taken == [e.sandbox_id for e in entries]
    assert all(e.state == "claimed" for e in _published(store, runtimes))


def _published(store, runtimes):
    """Every record the hosts publish once their pools settle."""
    assert _wait_until(lambda: not store.ready("img"))
    with store.lock:
        return [e for entries in store.hosts.values() for e in entries]


def test_a_store_needs_a_pool():
    with pytest.raises(ValueError, match="needs a warm_pool"):
        SandboxRuntime(warm_pool_store=_MemoryStore())


def test_the_default_host_outside_an_actor_is_the_host_and_process():
    assert not ray.is_initialized()
    assert _default_host() == f"{socket.gethostname()}:{os.getpid()}"


class _SqliteStore(WarmPoolStore):
    """A store in a database: one row per sandbox, replaced host by host."""

    _COLUMNS = ("host", "profile", "slot", "sandbox_id", "state", "node_id")
    _COLUMNS += ("since", "expires_at")

    def __init__(self, path):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._db.execute(
            "CREATE TABLE sandboxes (host TEXT, profile TEXT, slot INTEGER, "
            "sandbox_id TEXT PRIMARY KEY, state TEXT, node_id TEXT, since REAL, "
            "expires_at REAL)"
        )

    def publish(self, host, entries):
        rows = [tuple(getattr(e, c) for c in self._COLUMNS) for e in entries]
        with self._lock, self._db:
            self._db.execute("DELETE FROM sandboxes WHERE host = ?", (host,))
            self._db.executemany(
                "INSERT INTO sandboxes VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows
            )

    def ready(self, profile):
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM sandboxes WHERE profile = ? AND state = 'ready' "
                "AND expires_at > ?",
                (profile, time.time()),
            ).fetchall()
        return [WarmPoolEntry(*row) for row in rows]

    def rows(self):
        with self._lock:
            return self._db.execute(
                "SELECT slot, state FROM sandboxes ORDER BY slot"
            ).fetchall()


def test_a_database_can_hold_the_pool(tmp_path):
    """Every sandbox a pool holds is a row, ready or claimed, and goes away
    when the pool deletes it or closes."""
    store = _SqliteStore(str(tmp_path / "pool.db"))
    runtime = _pooled_runtime([{"image": "img", "size": 3}], store)
    assert _wait_until(lambda: store.rows() == [(i, "ready") for i in range(3)])
    taken = runtime.create(image="img", timeout_seconds=5)
    assert _wait_until(lambda: sorted(s for _, s in store.rows()).count("claimed") == 1)
    assert len(store.ready("img")) == 2
    assert taken not in {e.sandbox_id for e in store.ready("img")}
    runtime.close()
    assert store.rows() == []


@pytest.fixture
def ray_cluster():
    ray.init(num_cpus=4, include_dashboard=False)
    try:
        yield
    finally:
        ray.shutdown()


def _entry(host, slot, state="ready", expires_in=30.0, profile="img"):
    now = time.time()
    return WarmPoolEntry(
        host=host,
        profile=profile,
        slot=slot,
        sandbox_id=f"{host}-{slot}",
        state=state,
        node_id=None,
        since=now,
        expires_at=now + expires_in,
    )


def test_ray_warm_pool_store_keeps_every_hosts_entries(ray_cluster):
    store = RayWarmPoolStore(name=f"pool-{uuid.uuid4().hex[:8]}")
    store.publish("a", [_entry("a", 0), _entry("a", 1, state="claimed")])
    store.publish(
        "b", [_entry("b", 0), _entry("b", 1), _entry("b", 2, profile="other")]
    )
    store.publish("stale", [_entry("stale", 0, expires_in=-1)])
    # A host that dies with every sandbox claimed has no ready entries, and
    # still drops out once its records expire.
    store.publish("busy", [_entry("busy", 0, state="claimed", expires_in=-1)])
    ready = store.ready("img")
    assert collections.Counter(e.host for e in ready) == {"a": 1, "b": 2}
    assert len(store.ready("other")) == 1
    # Hosts that stopped publishing drop out; an empty publish removes one.
    assert {e.host for e in store.entries()} == {"a", "b"}
    store.publish("a", [])
    assert {e.host for e in store.ready("img")} == {"b"}
    # The store pickles to its name, so a copy in a task sees the same records.
    copy = pickle.loads(pickle.dumps(store))

    @ray.remote
    def ready_hosts(s):
        return sorted({e.host for e in s.ready("img")})

    assert ray.get(ready_hosts.remote(copy)) == ["b"]


def test_ray_warm_pool_store_spreads_callers_over_hosts(ray_cluster):
    """Callers between two publishes get the same entries. In a shuffled
    order, those that each take the first entry don't all pick one host."""
    store = RayWarmPoolStore(name=f"pool-{uuid.uuid4().hex[:8]}")
    for host in ("a", "b", "c", "d"):
        store.publish(host, [_entry(host, 0), _entry(host, 1)])
    firsts = {store.ready("img")[0].host for _ in range(40)}
    assert len(firsts) > 1
    assert len(store.ready("img")) == 8


def test_ray_warm_pool_store_replaces_an_actor_killed_for_good(ray_cluster):
    name = f"pool-{uuid.uuid4().hex[:8]}"
    store = RayWarmPoolStore(name=name)
    store.publish("a", [_entry("a", 0)])
    actor = ray.get_actor(name, namespace="ray_sandbox")
    ray.kill(actor, no_restart=True)

    def dead():
        try:
            ray.get(actor.entries.remote())
            return False
        except ray.exceptions.RayActorError:
            return True

    # ray.kill returns before the actor stops serving calls.
    assert _wait_until(dead, timeout_seconds=30)
    # The records went with it; the next publish fills the new actor.
    assert store.ready("img") == []
    store.publish("a", [_entry("a", 0)])
    assert [e.host for e in store.ready("img")] == ["a"]


def test_callers_route_creates_to_hosts_with_ready_sandboxes(ray_cluster):
    # Defined here so that workers get them by value, not by importing this
    # module.
    class FakeImageManager:
        def pull_image(self, image, timeout_seconds=120.0):
            return f"/tmp/fake-images/{image}"

    class FakeBackend:
        def __init__(self):
            self.booted = 0
            self.live = set()

        def create_sandbox(self, config):
            self.booted += 1
            self.live.add(f"sb-{self.booted:04d}")
            return f"sb-{self.booted:04d}"

        def delete_sandbox(self, sandbox_id):
            self.live.discard(sandbox_id)

        def get_status(self, sandbox_id):
            if sandbox_id in self.live:
                return SandboxStatus.RUNNING
            return SandboxStatus.TERMINATED

        def assign_config(self, sandbox_id, config):
            pass

    @ray.remote
    class PoolHost:
        def __init__(self, store):
            # One boot thread, so the fake backend needs no lock.
            from ray.experimental.sandbox import runtime as runtime_module

            runtime_module._WARM_BOOT_CONCURRENCY = 1
            self.runtime = runtime_module.SandboxRuntime()
            self.runtime._image_manager = FakeImageManager()
            self.runtime._backend = FakeBackend()
            self.runtime._warm = runtime_module._WarmPool(
                self.runtime, [{"image": "img", "name": "py", "size": 2}], store
            )

        def ready(self):
            return self.runtime.wait_for_warm_pool(timeout_seconds=10)

        def create(self):
            return self.runtime.create(image="img", timeout_seconds=5)

        def close(self):
            self.runtime.close()

    store = RayWarmPoolStore(name=f"pool-{uuid.uuid4().hex[:8]}")
    hosts = [PoolHost.options(name=f"host-{i}").remote(store) for i in range(2)]
    assert all(ray.get([h.ready.remote() for h in hosts]))
    # Each named actor publishes under its name.
    assert _wait_until(lambda: len(store.ready("py")) == 4, timeout_seconds=10)
    assert {e.host for e in store.ready("py")} == {"host-0", "host-1"}

    host, _ = collections.Counter(e.host for e in store.ready("py")).most_common(1)[0]
    before = {e.sandbox_id for e in store.ready("py") if e.host == host}
    taken = ray.get(ray.get_actor(host).create.remote())
    assert taken in before
    # Taking boots nothing, so the store shows one fewer ready.
    assert _wait_until(lambda: len(store.ready("py")) == 3, timeout_seconds=10)
    ray.get([h.close.remote() for h in hosts])
    assert _wait_until(lambda: store.ready("py") == [], timeout_seconds=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
