"""Tests for the NIXL memory pool statistics API.

These cover three layers, kept in separate sections so direct allocator tests
do not depend on a running Ray cluster or on the NIXL transport:

1. ``MemoryPoolManager.get_stats`` — deterministic, CPU-tensor-only tests of
   the snapshot arithmetic, the allocation-failure counter, and thread safety.
2. ``NixlTensorTransport.get_nixl_memory_pool_stats`` and
   ``register_nixl_memory_pool`` — publication, registration failure, and
   no-initialization behavior using a fake NIXL agent (no real NIXL).
3. ``get_nixl_memory_pool_stats`` public API — dispatch, validation, and the
   untouched/disabled/dead-actor and timeout behaviors.
"""

import dataclasses
import gc
import sys
import threading
import weakref

import pytest
import torch

import ray
from ray.experimental.rdt.nixl_memory_pool import (
    MemoryPoolManager,
    NixlMemoryPoolStats,
    NixlOutOfMemoryError,
)
from ray.experimental.rdt.nixl_tensor_transport import NixlTensorTransport
from ray.experimental.rdt.util import (
    _validate_nixl_pool_stats_timeout,
    get_nixl_memory_pool_stats,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_offsets(pool):
    """Return the (offset, size) pairs of a pool's free list, for assertions."""
    return [(b.offset, b.size) for b in pool._free_blocks]


def _fill_pool(pool, nbytes, stride=16):
    """Carve a pool into ``nbytes // stride`` ``stride``-byte objects.

    Returns the object ids so the caller can free specific ones to fragment the
    pool. ``stride`` must be a multiple of 16 (the pool's carve granularity) and
    ``nbytes`` must equal the pool size, so the pool ends up exactly full.
    """
    assert nbytes % stride == 0
    assert stride % 16 == 0
    obj_ids = []
    for i in range(nbytes // stride):
        obj_id = f"f{i}"
        pool.allocate_group(obj_id, [torch.zeros(stride, dtype=torch.uint8)])
        obj_ids.append(obj_id)
    return obj_ids


# ---------------------------------------------------------------------------
# MemoryPoolManager.get_stats — direct allocator tests
# ---------------------------------------------------------------------------


class TestSnapshotArithmetic:
    def test_empty_pool(self):
        pool = MemoryPoolManager(1024, torch.device("cpu"))
        s = pool.get_stats()
        assert isinstance(s, NixlMemoryPoolStats)
        assert s.device == "cpu"
        assert s.capacity_bytes == 1024
        assert s.allocated_bytes == 0
        assert s.free_bytes == 1024
        assert s.largest_free_block_bytes == 1024
        assert s.num_allocated_objects == 0
        assert s.num_free_blocks == 1
        assert s.allocation_failures == 0

    def test_capacity_zero(self):
        pool = MemoryPoolManager(0, torch.device("cpu"))
        s = pool.get_stats()
        assert s.capacity_bytes == 0
        assert s.allocated_bytes == 0
        assert s.free_bytes == 0
        assert s.largest_free_block_bytes == 0
        assert s.num_allocated_objects == 0
        # A zero-size free region is not a usable hole, so it is not counted.
        assert s.num_free_blocks == 0
        assert s.allocation_failures == 0

    def test_allocation_counts_alignment_padding(self):
        # float32 (4 bytes) then float64 (8 bytes): the float64 starts on an
        # 8-byte boundary, so 16 bytes are placed (4 + 4 pad + 8) for 12 bytes
        # of data, and the carved block rounds that up to the 16-byte boundary.
        pool = MemoryPoolManager(1024, torch.device("cpu"))
        pool.allocate_group(
            "o1",
            [
                torch.tensor([1.0], dtype=torch.float32),
                torch.tensor([2.0], dtype=torch.float64),
            ],
        )
        s = pool.get_stats()
        assert s.allocated_bytes == 16
        assert s.num_allocated_objects == 1
        assert s.free_bytes == 1024 - 16
        assert s.capacity_bytes == s.allocated_bytes + s.free_bytes

    def test_full_pool(self):
        pool = MemoryPoolManager(64, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(64, dtype=torch.uint8)])
        s = pool.get_stats()
        assert s.allocated_bytes == 64
        assert s.free_bytes == 0
        assert s.largest_free_block_bytes == 0
        assert s.num_free_blocks == 0

    def test_freed_pool_returns_to_empty(self):
        pool = MemoryPoolManager(128, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(16, dtype=torch.float32)])  # 64 B
        assert pool.free_object("o1")
        s = pool.get_stats()
        assert s.allocated_bytes == 0
        assert s.free_bytes == 128
        assert s.num_allocated_objects == 0
        assert s.num_free_blocks == 1

    def test_zero_size_tensor_still_counts_as_object(self):
        pool = MemoryPoolManager(128, torch.device("cpu"))
        pool.allocate_group("o1", [torch.empty(0, dtype=torch.float32)])
        s = pool.get_stats()
        assert s.num_allocated_objects == 1
        assert s.allocated_bytes == 0
        assert s.free_bytes == 128
        assert s.num_free_blocks == 1


class TestFragmentation:
    def test_largest_free_block_is_diagnostic(self):
        pool = MemoryPoolManager(96, torch.device("cpu"))
        obj_ids = _fill_pool(pool, 96, stride=32)
        pool.free_object(obj_ids[0])
        pool.free_object(obj_ids[2])
        s = pool.get_stats()
        # Two 32-byte holes with the middle 32 bytes still allocated.
        assert s.free_bytes == 64
        assert s.largest_free_block_bytes == 32
        assert s.num_free_blocks == 2

    def test_largest_hole_too_small_for_single_tensor(self):
        # Total free (32) exceeds the request (20), but no single hole is big
        # enough (each is 16 bytes), so the packed request fails and counts once.
        pool = MemoryPoolManager(64, torch.device("cpu"))
        obj_ids = _fill_pool(pool, 64, stride=16)
        pool.free_object(obj_ids[0])  # free 0..16
        pool.free_object(obj_ids[2])  # free 32..48
        assert sum(b.size for b in pool._free_blocks) == 32
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("big", [torch.zeros(5, dtype=torch.float32)])  # 20 B
        s = pool.get_stats()
        assert s.allocation_failures == 1

    def test_multi_tensor_failure_after_partial_placement(self):
        # The first tensor (8 B) is carved into a temporary 16-byte block, then
        # the second (20 B) cannot fit the remaining 16-byte hole. The group
        # fails, the counter increments once, and no state is committed.
        pool = MemoryPoolManager(64, torch.device("cpu"))
        obj_ids = _fill_pool(pool, 64, stride=16)
        pool.free_object(obj_ids[0])  # free 0..16
        pool.free_object(obj_ids[2])  # free 32..48
        free_before = _free_offsets(pool)
        allocated_before = {
            k: [(b.offset, b.size) for b in v]
            for k, v in pool._allocated_by_obj.items()
        }

        t1 = torch.zeros(2, dtype=torch.float32)  # 8 bytes
        t2 = torch.zeros(5, dtype=torch.float32)  # 20 bytes
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("g", [t1, t2])

        assert pool.get_stats().allocation_failures == 1
        assert _free_offsets(pool) == free_before
        assert {
            k: [(b.offset, b.size) for b in v]
            for k, v in pool._allocated_by_obj.items()
        } == allocated_before


class TestReplacementAndCounters:
    def test_successful_replacement(self):
        pool = MemoryPoolManager(64, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(2, dtype=torch.float32)])  # 8 -> 16 B
        assert pool.get_stats().allocated_bytes == 16
        pool.allocate_group("o1", [torch.zeros(8, dtype=torch.float32)])  # 32 B
        s = pool.get_stats()
        assert s.allocated_bytes == 32
        assert s.num_allocated_objects == 1
        assert s.allocation_failures == 0

    def test_failed_replacement_preserves_state_and_counts(self):
        pool = MemoryPoolManager(32, torch.device("cpu"))
        small = torch.zeros(2, dtype=torch.float32)  # 8 bytes -> carved 16
        huge = torch.zeros(20, dtype=torch.float32)  # 80 bytes > pool
        pool.allocate_group("o1", [small])
        free_before = _free_offsets(pool)
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("o1", [huge])
        s = pool.get_stats()
        assert s.allocation_failures == 1
        assert s.num_allocated_objects == 1
        assert s.allocated_bytes == 16  # prior allocation preserved
        assert _free_offsets(pool) == free_before

    def test_repeated_free_is_noop(self):
        pool = MemoryPoolManager(64, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(2, dtype=torch.float32)])
        assert pool.free_object("o1") is True
        assert pool.free_object("o1") is False
        s = pool.get_stats()
        assert s.num_allocated_objects == 0
        assert s.allocation_failures == 0

    def test_independent_pools_have_independent_counters(self):
        p1 = MemoryPoolManager(16, torch.device("cpu"))
        p2 = MemoryPoolManager(16, torch.device("cpu"))
        with pytest.raises(NixlOutOfMemoryError):
            p1.allocate_group("a", [torch.zeros(8, dtype=torch.float32)])  # 32 B
        assert p1.get_stats().allocation_failures == 1
        assert p2.get_stats().allocation_failures == 0

    def test_query_and_free_do_not_grow_counter(self):
        pool = MemoryPoolManager(64, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(4, dtype=torch.float32)])
        pool.get_stats()
        pool.free_object("o1")
        pool.get_stats()
        assert pool.get_stats().allocation_failures == 0

    def test_non_oom_exception_does_not_count(self):
        # A non-tensor input raises AttributeError (an input error), not
        # NixlOutOfMemoryError, so the failure counter must stay unchanged.
        pool = MemoryPoolManager(64, torch.device("cpu"))
        with pytest.raises(AttributeError):
            pool.allocate_group("o1", [object()])
        assert pool.get_stats().allocation_failures == 0
        assert pool.get_stats().num_allocated_objects == 0
        assert pool.get_stats().free_bytes == 64


class TestSnapshotImmutabilityAndThreadSafety:
    def test_snapshot_is_frozen_and_detached(self):
        pool = MemoryPoolManager(64, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(4, dtype=torch.float32)])  # 16 B
        s = pool.get_stats()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.allocated_bytes = 0
        # The snapshot is detached: later pool mutations don't rewrite it.
        pool.free_object("o1")
        assert s.allocated_bytes == 16
        assert s.num_allocated_objects == 1

    def test_snapshot_holds_no_tensor_reference(self):
        pool = MemoryPoolManager(64, torch.device("cpu"))
        pool.allocate_group("o1", [torch.zeros(4, dtype=torch.float32)])
        pool_tensor = pool.get_pool_tensor()
        ref = weakref.ref(pool_tensor)
        s = pool.get_stats()
        # Drop every owner of the pool tensor except the (scalar) snapshot.
        del pool_tensor
        del pool
        gc.collect()
        assert ref() is None, "snapshot retained a strong tensor reference"
        assert s.device == "cpu"
        assert s.allocated_bytes == 16

    def test_concurrent_snapshots_have_coherent_sums(self):
        pool = MemoryPoolManager(8192, torch.device("cpu"))
        errors = []
        stop = threading.Event()

        def allocator(worker_id):
            try:
                for i in range(200):
                    pool.allocate_group(
                        f"w{worker_id}-{i}",
                        [torch.zeros(16, dtype=torch.float32)],  # 64 B
                    )
                    pool.free_object(f"w{worker_id}-{i}")
            except Exception as e:  # pragma: no cover - asserted below
                errors.append(e)

        def sampler():
            try:
                while not stop.is_set():
                    s = pool.get_stats()
                    if s.capacity_bytes != s.allocated_bytes + s.free_bytes:
                        errors.append(AssertionError(f"invariant violated: {s}"))
            except Exception as e:  # pragma: no cover - asserted below
                errors.append(e)

        allocators = [threading.Thread(target=allocator, args=(i,)) for i in range(4)]
        samplers = [threading.Thread(target=sampler) for _ in range(2)]
        for t in allocators + samplers:
            t.start()
        for t in allocators:
            t.join()
        stop.set()
        for t in samplers:
            t.join()

        assert errors == []
        s = pool.get_stats()
        assert s.num_allocated_objects == 0
        assert s.free_bytes == 8192


# ---------------------------------------------------------------------------
# NixlTensorTransport — publication and registration behavior (fake agent)
# ---------------------------------------------------------------------------


class _FakeNixlAgent:
    def __init__(self, fail=False):
        self.fail = fail
        self.registered = []

    def register_memory(self, tensor):
        if self.fail:
            raise RuntimeError("register failed")
        self.registered.append(tensor)


class TestNixlTransportStats:
    def test_no_pool_returns_none(self):
        transport = NixlTensorTransport()
        assert transport.get_nixl_memory_pool_stats() is None

    def test_register_pool_then_snapshot(self):
        transport = NixlTensorTransport()
        transport._nixl_agent = _FakeNixlAgent()
        transport.register_nixl_memory_pool(1024, torch.device("cpu"))
        s = transport.get_nixl_memory_pool_stats()
        assert s is not None
        assert s.device == "cpu"
        assert s.capacity_bytes == 1024
        assert len(transport._nixl_agent.registered) == 1

    def test_failed_registration_leaves_pool_unset(self):
        transport = NixlTensorTransport()
        transport._nixl_agent = _FakeNixlAgent(fail=True)
        with pytest.raises(RuntimeError, match="register failed"):
            transport.register_nixl_memory_pool(1024, torch.device("cpu"))
        assert transport.get_nixl_memory_pool_stats() is None

    def test_double_registration_raises_and_keeps_pool(self):
        transport = NixlTensorTransport()
        transport._nixl_agent = _FakeNixlAgent()
        transport.register_nixl_memory_pool(1024, torch.device("cpu"))
        pool_before = transport._memory_pool
        with pytest.raises(ValueError, match="already registered"):
            transport.register_nixl_memory_pool(2048, torch.device("cpu"))
        assert transport._memory_pool is pool_before
        assert transport.get_nixl_memory_pool_stats().capacity_bytes == 1024

    def test_concurrent_registration_cannot_replace_pool(self):
        # The whole check/create/register/publish runs under the cache lock, so
        # exactly one of two concurrent registrations succeeds and the loser
        # raises without overwriting the winner's pool.
        transport = NixlTensorTransport()
        transport._nixl_agent = _FakeNixlAgent()
        barrier = threading.Barrier(2)
        results = []

        def register(size):
            barrier.wait()
            try:
                transport.register_nixl_memory_pool(size, torch.device("cpu"))
                results.append(("ok", size))
            except ValueError:
                results.append(("err", size))

        sizes = [1024, 2048]
        threads = [threading.Thread(target=register, args=(s,)) for s in sizes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        oks = [r for r in results if r[0] == "ok"]
        errs = [r for r in results if r[0] == "err"]
        assert len(oks) == 1 and len(errs) == 1, results
        assert transport.get_nixl_memory_pool_stats().capacity_bytes == oks[0][1]


# ---------------------------------------------------------------------------
# get_nixl_memory_pool_stats — public API dispatch and validation
# ---------------------------------------------------------------------------


def test_validate_timeout():
    _validate_nixl_pool_stats_timeout(0.1)
    _validate_nixl_pool_stats_timeout(10.0)
    for bad in [0, -1, 0.0, -0.5, float("inf"), float("-inf"), float("nan")]:
        with pytest.raises(ValueError):
            _validate_nixl_pool_stats_timeout(bad)
    # Non-numeric and bool are rejected without echoing the value (a huge int
    # must not hit Python's integer-to-string digit limit or OverflowError).
    for bad in [None, "10", True, False, 10**1000, -(10**1000)]:
        with pytest.raises(ValueError):
            _validate_nixl_pool_stats_timeout(bad)
    for bad in [3601, 1e9]:
        with pytest.raises(ValueError):
            _validate_nixl_pool_stats_timeout(bad)


def test_rejects_non_actor():
    with pytest.raises(TypeError, match="actor handle"):
        get_nixl_memory_pool_stats(object())
    with pytest.raises(TypeError, match="actor handle"):
        get_nixl_memory_pool_stats(None)


@ray.remote(enable_tensor_transport=True)
class NixlPoolStatsActor:
    def create_transport_only(self):
        # Create the NIXL transport in this actor's process without a pool.
        from ray.experimental.rdt.util import get_tensor_transport_manager

        get_tensor_transport_manager("NIXL")

    def register_pool(self, size=1024):
        # Register a CPU pool on this actor using a fake NIXL agent, so the
        # public getter can be exercised end-to-end without real NIXL. The fake
        # agent is defined locally so this method carries no reference to the
        # test module (which is not importable on the worker).
        import torch

        from ray.experimental.rdt.util import get_tensor_transport_manager

        class _FakeAgent:
            def register_memory(self, tensor):
                pass

        transport = get_tensor_transport_manager("NIXL")
        transport._nixl_agent = _FakeAgent()
        transport.register_nixl_memory_pool(size, torch.device("cpu"))

    def allocate(self, obj_id, numel):
        # Carve a live object into the registered pool so a later snapshot
        # reports non-zero allocated_bytes through the public API.
        import torch

        from ray.experimental.rdt.util import get_tensor_transport_manager

        transport = get_tensor_transport_manager("NIXL")
        transport._memory_pool.allocate_group(
            obj_id, [torch.zeros(numel, dtype=torch.float32)]
        )

    def transport_state(self):
        from ray.experimental.rdt import util as _rdt_util

        with _rdt_util.transport_managers_lock:
            transport = _rdt_util.transport_managers.get("NIXL")
        return {
            "has_nixl_transport": transport is not None,
            "has_pool": transport is not None and transport._memory_pool is not None,
        }

    def ready(self):
        return True


@ray.remote
class PlainNixlPoolStatsActor:
    pass


@ray.remote(enable_tensor_transport=True)
class BlockingNixlPoolStatsActor:
    def __init__(self):
        self._started = False

    @ray.method(concurrency_group="_ray_system")
    def block_background(self):
        self._started = True
        import time

        time.sleep(30)

    def started(self):
        return self._started


def test_untouched_actor_returns_none(ray_start_regular):
    actor = NixlPoolStatsActor.remote()
    assert ray.get(actor.ready.remote()) is True
    assert get_nixl_memory_pool_stats(actor) is None


def test_public_api_roundtrip_returns_registered_pool_snapshot(ray_start_regular):
    # The full public path: register a pool on the actor, then query it from
    # the driver and get back a non-None snapshot whose fields match the pool,
    # including a live allocation made after registration.
    actor = NixlPoolStatsActor.remote()
    ray.get(actor.register_pool.remote(1024), timeout=10)

    stats = get_nixl_memory_pool_stats(actor)
    assert isinstance(stats, NixlMemoryPoolStats)
    assert stats.device == "cpu"
    assert stats.capacity_bytes == 1024
    assert stats.allocated_bytes == 0
    assert stats.free_bytes == 1024
    assert stats.largest_free_block_bytes == 1024
    assert stats.num_allocated_objects == 0
    assert stats.num_free_blocks == 1
    assert stats.allocation_failures == 0

    # A live allocation is reflected in a later snapshot (16 float32 = 64 B).
    ray.get(actor.allocate.remote("o1", 16), timeout=10)
    stats = get_nixl_memory_pool_stats(actor)
    assert stats.allocated_bytes == 64
    assert stats.free_bytes == 1024 - 64
    assert stats.num_allocated_objects == 1
    assert stats.num_free_blocks == 1
    assert stats.capacity_bytes == stats.allocated_bytes + stats.free_bytes


def test_untouched_actor_does_not_initialize(ray_start_regular):
    actor = NixlPoolStatsActor.remote()
    before = ray.get(actor.transport_state.remote())
    assert before == {"has_nixl_transport": False, "has_pool": False}
    assert get_nixl_memory_pool_stats(actor) is None
    after = ray.get(actor.transport_state.remote())
    # Querying an untouched actor must not create a transport, agent, or pool.
    assert after == {"has_nixl_transport": False, "has_pool": False}


def test_transport_without_pool_returns_none(ray_start_regular):
    actor = NixlPoolStatsActor.remote()
    ray.get(actor.create_transport_only.remote())
    # NIXL transport exists in the actor but no pool was registered, so the
    # result is None (distinct from a registered-but-empty pool).
    assert get_nixl_memory_pool_stats(actor) is None


def test_requires_tensor_transport(ray_start_regular):
    actor = PlainNixlPoolStatsActor.remote()
    with pytest.raises(ValueError, match="enable_tensor_transport=True"):
        get_nixl_memory_pool_stats(actor)


@pytest.mark.parametrize(
    "bad_timeout",
    [0, -1, float("inf"), float("nan"), None, "10", True, 10**1000, -(10**1000)],
)
def test_invalid_timeout(ray_start_regular, bad_timeout):
    actor = NixlPoolStatsActor.remote()
    with pytest.raises(ValueError):
        get_nixl_memory_pool_stats(actor, timeout=bad_timeout)


def test_timeout_bounds_wait_does_not_cancel(ray_start_regular):
    from ray._common.test_utils import wait_for_condition

    actor = BlockingNixlPoolStatsActor.remote()
    block_ref = actor.block_background.remote()
    # Ensure the blocking task has grabbed the single-threaded _ray_system group
    # before submitting the snapshot, so the snapshot queues behind it.
    wait_for_condition(lambda: ray.get(actor.started.remote()), timeout=10)
    with pytest.raises(ray.exceptions.GetTimeoutError):
        get_nixl_memory_pool_stats(actor, timeout=0.5)
    # Keep the blocking task's ref alive through the assertion.
    assert block_ref is not None


def test_dead_actor(ray_start_regular):
    actor = NixlPoolStatsActor.remote()
    ray.kill(actor)
    with pytest.raises(ray.exceptions.RayActorError):
        get_nixl_memory_pool_stats(actor)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
