import os
import sys
from unittest import mock

import pytest

import ray
from ray.experimental.rdt import nic_allocator
from ray.experimental.rdt.nic_allocator import (
    RDT_NIC_PINNING_ENV_VAR,
    NICAllocator,
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


def _make_sysfs(tmp_path, devices):
    """Build a fake /sys/class/infiniband tree: {device: [ports]}."""
    root = tmp_path / "infiniband"
    for dev, ports in devices.items():
        for port in ports:
            (root / dev / "ports" / str(port)).mkdir(parents=True)
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


class TestNICAllocatorLogic:
    """Drive the allocator's underlying class directly, no cluster needed."""

    def _allocator(self):
        return NICAllocator.__ray_metadata__.modified_class()

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
