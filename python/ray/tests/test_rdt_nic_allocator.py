import os
import sys

import pytest

import ray
from ray.experimental.rdt import nic_allocator
from ray.experimental.rdt.nic_allocator import (
    RDT_NIC_PINNING_ENV_VAR,
    NICAllocator,
    acquire_nic_for_current_actor,
    discover_rdma_nics,
)


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
