import os
import sys
from unittest import mock

import pytest

import ray
from ray._private import rdt_nic_allocator as nic_allocator
from ray._private.rdt_nic_allocator import (
    _KV_NAMESPACE,
    _KV_STATE_KEY,
    RDT_NIC_PINNING_ENV_VAR,
    _NICAllocatorImpl,
    acquire_nic_for_current_actor,
    discover_rdma_nics,
    release_nic_for_current_actor,
)


@pytest.fixture(autouse=True)
def _reset_acquired_nic_state():
    """_acquired_nic is process-local global state; isolate tests from it."""
    nic_allocator._acquired_nic = None
    yield
    nic_allocator._acquired_nic = None


def _make_sysfs(tmp_path, devices, states=None):
    """Build a fake /sys/class/infiniband tree: {device: [ports]}.

    Each port gets a ``state`` file containing "4: ACTIVE" by default,
    matching a healthy link. Pass ``states={(dev, port): "1: DOWN"}`` to
    override specific ports, or map to None to omit the state file
    entirely (simulating an unreadable/missing sysfs entry).
    """
    states = states or {}
    root = tmp_path / "infiniband"
    for dev, ports in devices.items():
        for port in ports:
            port_dir = root / dev / "ports" / str(port)
            port_dir.mkdir(parents=True)
            state = states.get((dev, port), "4: ACTIVE")
            if state is not None:
                (port_dir / "state").write_text(state)
    return str(root)


@pytest.mark.parametrize(
    "devices,expected",
    [
        ({}, []),
        ({"mlx5_0": [1]}, ["mlx5_0:1"]),
        ({"mlx5_0": [1, 2]}, ["mlx5_0:1", "mlx5_0:2"]),
        (
            {"mlx5_1": [1], "mlx5_0": [1], "mlx5_2": [1]},
            ["mlx5_0:1", "mlx5_1:1", "mlx5_2:1"],
        ),
    ],
)
def test_discover_rdma_nics(tmp_path, monkeypatch, devices, expected):
    """Discovery returns sorted, port-qualified names; empty without devices."""
    monkeypatch.setattr(
        nic_allocator, "_INFINIBAND_SYSFS_ROOT", _make_sysfs(tmp_path, devices)
    )
    assert discover_rdma_nics() == expected


def test_discover_rdma_nics_missing_root(monkeypatch):
    """A host without the sysfs tree (e.g. Windows/macOS) yields no NICs."""
    monkeypatch.setattr(
        nic_allocator, "_INFINIBAND_SYSFS_ROOT", "/nonexistent/infiniband"
    )
    assert discover_rdma_nics() == []


def test_discover_rdma_nics_excludes_down_ports(tmp_path, monkeypatch):
    """A DOWN port must never be handed out for exclusive pinning -- doing
    so would steer UCX at a device that can't move data, which is worse
    than the unpinned default this feature must never regress below."""
    sysfs_root = _make_sysfs(
        tmp_path,
        {"mlx5_0": [1, 2]},
        states={("mlx5_0", 2): "1: DOWN"},
    )
    monkeypatch.setattr(nic_allocator, "_INFINIBAND_SYSFS_ROOT", sysfs_root)
    assert discover_rdma_nics() == ["mlx5_0:1"]


def test_discover_rdma_nics_excludes_unreadable_state(tmp_path, monkeypatch):
    """A port with a missing/unreadable state file is treated as inactive,
    not active -- fail open by excluding a possibly-fine port rather than
    risking a pin to a possibly-dead one."""
    sysfs_root = _make_sysfs(
        tmp_path,
        {"mlx5_0": [1, 2]},
        states={("mlx5_0", 2): None},
    )
    monkeypatch.setattr(nic_allocator, "_INFINIBAND_SYSFS_ROOT", sysfs_root)
    assert discover_rdma_nics() == ["mlx5_0:1"]


def test_discover_rdma_nics_all_down_yields_empty(tmp_path, monkeypatch):
    """If every port is down, discovery must return no NICs rather than
    falling back to pinning something -- acquire_nic_for_current_actor's
    empty-list check then correctly leaves UCX unpinned."""
    sysfs_root = _make_sysfs(
        tmp_path,
        {"mlx5_0": [1]},
        states={("mlx5_0", 1): "1: DOWN"},
    )
    monkeypatch.setattr(nic_allocator, "_INFINIBAND_SYSFS_ROOT", sysfs_root)
    assert discover_rdma_nics() == []


class TestNICAllocatorLogic:
    """Drive the allocator's underlying class directly, no cluster needed."""

    @pytest.fixture(autouse=True)
    def _force_kv_uninitialized(self, monkeypatch):
        # These tests exercise pure in-memory logic and must behave the
        # same regardless of whether some other test in this session left
        # Ray's internal KV client initialized. Forcing it uninitialized
        # here keeps _load_persisted_state/_persist_state as no-ops, same
        # as a genuinely cluster-free instantiation.
        monkeypatch.setattr(
            "ray.experimental.internal_kv._internal_kv_initialized",
            lambda: False,
        )

    def _allocator(self):
        # The actor class is wrapped lazily in ray.remote() only when a real
        # allocator handle is needed; here we exercise the plain
        # implementation directly, with no cluster required.
        return _NICAllocatorImpl()

    def test_acquire_release_cycle(self):
        alloc = self._allocator()
        alloc.register_node("node1", ["mlx5_0:1", "mlx5_1:1"])

        nic_a = alloc.acquire("node1", "actorA")
        nic_b = alloc.acquire("node1", "actorB")
        assert {nic_a, nic_b} == {"mlx5_0:1", "mlx5_1:1"}

        # Exhausted: third actor gets nothing.
        assert alloc.acquire("node1", "actorC") is None

        # Release frees a NIC for the waiter.
        alloc.release("node1", "actorA")
        assert alloc.acquire("node1", "actorC") == nic_a

    def test_acquire_is_reentrant(self):
        alloc = self._allocator()
        alloc.register_node("node1", ["mlx5_0:1", "mlx5_1:1"])
        first = alloc.acquire("node1", "actorA")
        # Re-acquiring returns the same NIC instead of leaking a second one.
        assert alloc.acquire("node1", "actorA") == first
        snapshot = alloc.snapshot()
        assert sum(v == "actorA" for v in snapshot["node1"].values()) == 1

    def test_register_node_is_idempotent(self):
        alloc = self._allocator()
        alloc.register_node("node1", ["mlx5_0:1"])
        assert alloc.acquire("node1", "actorA") == "mlx5_0:1"
        # A later registration must not wipe the live assignment.
        alloc.register_node("node1", ["mlx5_0:1"])
        assert alloc.snapshot()["node1"]["mlx5_0:1"] == "actorA"

    def test_unknown_node(self):
        alloc = self._allocator()
        assert alloc.acquire("ghost", "actorA") is None
        alloc.release("ghost", "actorA")  # must not raise

    def test_release_all_for_node(self):
        alloc = self._allocator()
        alloc.register_node("node1", ["mlx5_0:1", "mlx5_1:1"])
        alloc.acquire("node1", "actorA")
        alloc.acquire("node1", "actorB")
        alloc.release_all_for_node("node1")
        assert all(v is None for v in alloc.snapshot()["node1"].values())


class _FakeKV:
    """In-memory stand-in for Ray's internal KV store, keyed by (key, ns)."""

    def __init__(self):
        self.store = {}

    def get(self, key, *, namespace=None):
        return self.store.get((key, namespace))

    def put(self, key, value, *args, namespace=None, **kwargs):
        self.store[(key, namespace)] = value
        return False


class TestNICAllocatorPersistence:
    """Verify state survives across allocator instances via the KV store."""

    @pytest.fixture(autouse=True)
    def _fake_kv(self, monkeypatch):
        kv = _FakeKV()
        monkeypatch.setattr(
            "ray.experimental.internal_kv._internal_kv_initialized", lambda: True
        )
        monkeypatch.setattr("ray.experimental.internal_kv._internal_kv_get", kv.get)
        monkeypatch.setattr("ray.experimental.internal_kv._internal_kv_put", kv.put)
        self.kv = kv
        yield

    def test_state_survives_across_instances(self):
        """A second instance (standing in for a restarted/re-created
        allocator) must rehydrate exactly what the first one persisted."""
        first = _NICAllocatorImpl()
        first.register_node("node1", ["mlx5_0:1", "mlx5_1:1"])
        nic = first.acquire("node1", "actorA")
        assert nic == "mlx5_0:1"

        second = _NICAllocatorImpl()
        assert second.snapshot() == first.snapshot()
        # The surviving assignment must still be respected: a new actor
        # can't be handed the NIC actorA already holds.
        assert second.acquire("node1", "actorB") == "mlx5_1:1"

    def test_release_is_persisted(self):
        first = _NICAllocatorImpl()
        first.register_node("node1", ["mlx5_0:1"])
        first.acquire("node1", "actorA")
        first.release("node1", "actorA")

        second = _NICAllocatorImpl()
        assert second.snapshot()["node1"]["mlx5_0:1"] is None

    def test_corrupt_kv_state_falls_back_to_empty(self):
        """A corrupt/unparseable persisted value must not crash startup --
        degrade to an empty registry, same as no KV entry at all."""
        self.kv.store[(_KV_STATE_KEY, _KV_NAMESPACE)] = b"not valid json {{{"
        alloc = _NICAllocatorImpl()
        assert alloc.snapshot() == {}

    def test_kv_uninitialized_yields_empty_registry(self, monkeypatch):
        """No live cluster (KV never initialized): starts empty, no crash."""
        monkeypatch.setattr(
            "ray.experimental.internal_kv._internal_kv_initialized", lambda: False
        )
        alloc = _NICAllocatorImpl()
        assert alloc.snapshot() == {}


def test_acquire_noop_when_disabled(monkeypatch):
    """With the flag unset, acquisition returns None without touching Ray."""
    monkeypatch.delenv(RDT_NIC_PINNING_ENV_VAR, raising=False)
    assert acquire_nic_for_current_actor() is None


def test_acquire_noop_without_nics(monkeypatch):
    """Flag on but no RDMA hardware: fail open before any Ray calls."""
    monkeypatch.setenv(RDT_NIC_PINNING_ENV_VAR, "1")
    monkeypatch.setattr(
        nic_allocator, "_INFINIBAND_SYSFS_ROOT", "/nonexistent/infiniband"
    )
    assert acquire_nic_for_current_actor() is None


def test_release_noop_without_acquired_nic(monkeypatch):
    """release() must short-circuit before touching Ray when this process
    never recorded a successful acquire, even if pinning is enabled."""
    monkeypatch.setenv(RDT_NIC_PINNING_ENV_VAR, "1")
    assert nic_allocator._acquired_nic is None
    with mock.patch("ray.get_actor") as mock_get_actor:
        release_nic_for_current_actor()
    mock_get_actor.assert_not_called()


def test_release_preserves_acquired_nic_on_failure(monkeypatch):
    """If the release RPC fails, _acquired_nic must NOT be cleared: the
    allocator may still show this process as the owner, so erasing the
    local record here would silence any future retry and leak the NIC."""
    monkeypatch.setenv(RDT_NIC_PINNING_ENV_VAR, "1")
    nic_allocator._acquired_nic = "mlx5_0:1"

    with (
        mock.patch("ray.get_actor", side_effect=RuntimeError("allocator unreachable")),
        mock.patch("ray.get_runtime_context") as mock_ctx,
    ):
        mock_ctx.return_value.get_actor_id.return_value = "actorA"
        mock_ctx.return_value.get_node_id.return_value = "node1"
        release_nic_for_current_actor()

    assert nic_allocator._acquired_nic == "mlx5_0:1"


def test_shutdown_releases_nic_even_if_flag_cleared(monkeypatch):
    """release_nic_for_current_actor's own decision to act must depend only
    on whether this process actually holds a NIC (_acquired_nic), never on
    the current value of RAY_RDT_NIC_PINNING -- the env var can be changed
    at any time and is unrelated to whether a NIC was already acquired."""
    # Simulate: NIC was acquired earlier while the flag was on...
    nic_allocator._acquired_nic = "mlx5_0:1"
    # ...then the flag got cleared before shutdown runs.
    monkeypatch.delenv(RDT_NIC_PINNING_ENV_VAR, raising=False)

    released = {}

    class FakeAllocatorHandle:
        release = mock.Mock()
        release.remote = mock.Mock(
            side_effect=lambda node_id, actor_id: released.update(
                node_id=node_id, actor_id=actor_id
            )
        )

    with (
        mock.patch("ray.get_actor", return_value=FakeAllocatorHandle()),
        mock.patch("ray.get_runtime_context") as mock_ctx,
        mock.patch("ray.get", return_value=None),
    ):
        mock_ctx.return_value.get_actor_id.return_value = "actorA"
        mock_ctx.return_value.get_node_id.return_value = "node1"
        release_nic_for_current_actor()

    assert released == {"node_id": "node1", "actor_id": "actorA"}
    assert nic_allocator._acquired_nic is None


def test_release_clears_acquired_nic_after_success(ray_start_regular, tmp_path):
    """A successful acquire followed by release clears the local record and
    frees the NIC for another actor on the same node."""
    sysfs_root = _make_sysfs(tmp_path, {"mlx5_0": [1]})

    @ray.remote
    class RDTWorker:
        def __init__(self, sysfs_root):
            os.environ[RDT_NIC_PINNING_ENV_VAR] = "1"
            nic_allocator._INFINIBAND_SYSFS_ROOT = sysfs_root

        def acquire(self):
            return acquire_nic_for_current_actor()

        def acquired_nic_is_recorded(self):
            return nic_allocator._acquired_nic is not None

        def release(self):
            nic_allocator.release_nic_for_current_actor()
            return nic_allocator._acquired_nic

    w1 = RDTWorker.remote(sysfs_root)
    w2 = RDTWorker.remote(sysfs_root)

    assert ray.get(w1.acquire.remote()) == "mlx5_0:1"
    assert ray.get(w1.acquired_nic_is_recorded.remote()) is True
    # Only one NIC exists, so a second actor gets nothing until released.
    assert ray.get(w2.acquire.remote()) is None

    assert ray.get(w1.release.remote()) is None  # cleared after release
    assert ray.get(w2.acquire.remote()) == "mlx5_0:1"


def test_reentrant_acquire_short_circuits_without_rpc(monkeypatch):
    """A re-entrant acquire (e.g. NIXL agent rebuild) for a process that
    already holds a NIC must return it immediately, with no RPC at all.

    Without this short-circuit, a slow register_node/acquire on the
    re-entrant call could time out and trigger the orphan-release path,
    which would release the NIC this process is already actively using --
    letting another actor take it while this one keeps it pinned.
    """
    monkeypatch.setenv(RDT_NIC_PINNING_ENV_VAR, "1")
    nic_allocator._acquired_nic = "mlx5_0:1"

    with (
        mock.patch("ray.get_actor") as mock_get_actor,
        mock.patch("ray.get") as mock_get,
        mock.patch.object(nic_allocator, "discover_rdma_nics") as mock_discover,
    ):
        result = acquire_nic_for_current_actor()

    assert result == "mlx5_0:1"
    mock_get_actor.assert_not_called()
    mock_get.assert_not_called()
    mock_discover.assert_not_called()


def test_acquire_timeout_releases_possible_orphan(monkeypatch):
    """A client-side timeout on the acquire RPC must not leave a permanent
    reservation: the caller should fire a best-effort release rather than
    silently leaking the NIC (the remote call may have already committed
    even though the client gave up waiting on it)."""
    monkeypatch.setenv(RDT_NIC_PINNING_ENV_VAR, "1")
    monkeypatch.setattr(nic_allocator, "discover_rdma_nics", lambda: ["mlx5_0:1"])

    released = {}

    class FakeAllocatorHandle:
        def __init__(self):
            self.register_node = mock.Mock()
            self.register_node.remote = mock.Mock(return_value="register_ref")
            self.acquire = mock.Mock()
            self.acquire.remote = mock.Mock(return_value="acquire_ref")
            self.release = mock.Mock()

            def _release_remote(node_id, actor_id):
                released["node_id"] = node_id
                released["actor_id"] = actor_id

            self.release.remote = mock.Mock(side_effect=_release_remote)

    fake_allocator = FakeAllocatorHandle()
    monkeypatch.setattr(
        nic_allocator, "_get_or_create_allocator", lambda: fake_allocator
    )

    def fake_ray_get(ref, timeout=None):
        if ref == "register_ref":
            return None
        if ref == "acquire_ref":
            # Simulate the register_node call succeeding but the acquire
            # call's client-side wait timing out.
            raise ray.exceptions.GetTimeoutError("simulated timeout")
        raise AssertionError(f"unexpected ray.get call on {ref!r}")

    with (
        mock.patch("ray.get", side_effect=fake_ray_get),
        mock.patch("ray.get_runtime_context") as mock_ctx,
    ):
        mock_ctx.return_value.get_actor_id.return_value = "actorA"
        mock_ctx.return_value.get_node_id.return_value = "node1"
        result = acquire_nic_for_current_actor()

    assert result is None
    assert nic_allocator._acquired_nic is None
    fake_allocator.release.remote.assert_called_once_with("node1", "actorA")
    assert released == {"node_id": "node1", "actor_id": "actorA"}


def test_end_to_end_exclusive_assignment(ray_start_regular, tmp_path):
    """4 actors contend for 2 fake NICs: exactly 2 win, release frees a slot."""
    sysfs_root = _make_sysfs(tmp_path, {"mlx5_0": [1], "mlx5_1": [1]})

    @ray.remote
    class RDTWorker:
        def __init__(self, sysfs_root):
            os.environ[RDT_NIC_PINNING_ENV_VAR] = "1"
            nic_allocator._INFINIBAND_SYSFS_ROOT = sysfs_root

        def acquire(self):
            return acquire_nic_for_current_actor()

        def release(self):
            nic_allocator.release_nic_for_current_actor()

    workers = [RDTWorker.remote(sysfs_root) for _ in range(4)]
    nics = ray.get([w.acquire.remote() for w in workers])

    assigned = [n for n in nics if n is not None]
    assert sorted(assigned) == ["mlx5_0:1", "mlx5_1:1"]
    assert nics.count(None) == 2

    # Releasing one lets a previously rejected worker acquire.
    winner = next(w for w, n in zip(workers, nics) if n is not None)
    loser = next(w for w, n in zip(workers, nics) if n is None)
    ray.get(winner.release.remote())
    assert ray.get(loser.acquire.remote()) in ["mlx5_0:1", "mlx5_1:1"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
