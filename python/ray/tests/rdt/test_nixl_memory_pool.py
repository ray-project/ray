"""Unit tests for MemoryPoolManager.
"""

import sys

import pytest
import torch

from ray.experimental.rdt.nixl_memory_pool import (
    MemoryPoolManager,
    NixlOutOfMemoryError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tensor(values, dtype=torch.float32):
    """Create a contiguous CPU tensor."""
    return torch.tensor(values, dtype=dtype)


def _storage_ptr(t: torch.Tensor) -> int:
    return t.untyped_storage().data_ptr()


# ---------------------------------------------------------------------------
# allocate_for_tensors — basic allocation and data copy
# ---------------------------------------------------------------------------


class TestAllocateForTensors:
    def test_single_tensor(self):
        t = _make_tensor([1.0, 2.0, 3.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([t])

        assert len(views) == 1
        assert torch.equal(views[0], t)
        assert pool.has_block(_storage_ptr(t))

    def test_multiple_independent_tensors(self):
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0, 5.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([t1, t2])

        assert len(views) == 2
        assert torch.equal(views[0], t1)
        assert torch.equal(views[1], t2)
        assert pool.has_block(_storage_ptr(t1))
        assert pool.has_block(_storage_ptr(t2))

    def test_pool_views_are_backed_by_pool_tensor(self):
        """Returned views should be backed by the pool's internal tensor,
        not the source tensor's storage."""
        t = _make_tensor([10.0, 20.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([t])

        # The view's storage should be the pool tensor's storage.
        assert (
            views[0].untyped_storage().data_ptr()
            == pool.get_pool_tensor().untyped_storage().data_ptr()
        )

    def test_data_is_copied_not_aliased(self):
        """Mutating the source tensor after allocation should not affect
        the pool copy."""
        t = _make_tensor([1.0, 2.0, 3.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([t])

        original = views[0].clone()
        t[0] = 999.0
        assert torch.equal(views[0], original)


# ---------------------------------------------------------------------------
# allocate_for_tensors — storage deduplication
# ---------------------------------------------------------------------------


class TestStorageDeduplication:
    def test_views_of_same_storage_share_one_block(self):
        """Two views of the same underlying storage should produce only one
        pool allocation."""
        base = _make_tensor([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        view_a = base[0:2]
        view_b = base[1:3]

        storage_size = base.untyped_storage().nbytes()
        # Pool is exactly one storage — a second allocation would OOM.
        pool = MemoryPoolManager(pool_size=storage_size, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([view_a, view_b])

        assert len(views) == 2
        assert torch.equal(views[0], view_a)
        assert torch.equal(views[1], view_b)

    def test_duplicate_tensor_in_list(self):
        """The exact same tensor object appearing twice should deduplicate."""
        t = _make_tensor([1.0, 2.0])
        storage_size = t.untyped_storage().nbytes()
        pool = MemoryPoolManager(pool_size=storage_size, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([t, t])

        assert len(views) == 2
        assert torch.equal(views[0], t)
        assert torch.equal(views[1], t)

    def test_cross_call_reuse(self):
        """A second allocate_for_tensors call with the same tensor should
        reuse the existing pool block (cache hit), not allocate a new one."""
        t = _make_tensor([1.0, 2.0, 3.0])
        storage_size = t.untyped_storage().nbytes()
        # Pool fits exactly one storage.
        pool = MemoryPoolManager(pool_size=storage_size, device=torch.device("cpu"))

        views1 = pool.allocate_for_tensors([t])
        # Second call should hit cache, not OOM.
        views2 = pool.allocate_for_tensors([t])

        assert torch.equal(views1[0], t)
        assert torch.equal(views2[0], t)

    def test_mixed_cache_hit_and_new_allocation(self):
        """One call with a mix of already-allocated and new tensors should
        only allocate for the new ones."""
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0, 5.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))

        # Pre-allocate t1.
        pool.allocate_for_tensors([t1])

        # Now allocate both — t1 should cache-hit, t2 should get new block.
        views = pool.allocate_for_tensors([t1, t2])
        assert len(views) == 2
        assert torch.equal(views[0], t1)
        assert torch.equal(views[1], t2)
        assert pool.has_block(_storage_ptr(t2))


# ---------------------------------------------------------------------------
# allocate_for_tensors — OOM
# ---------------------------------------------------------------------------


class TestOOM:
    def test_oom_single_tensor(self):
        t = _make_tensor([1.0, 2.0, 3.0])  # 12 bytes
        pool = MemoryPoolManager(pool_size=4, device=torch.device("cpu"))

        with pytest.raises(NixlOutOfMemoryError, match="out of memory"):
            pool.allocate_for_tensors([t])

    def test_oom_does_not_corrupt_pool_state(self):
        """After an OOM error, the pool state should be unchanged — previously
        allocated blocks remain valid and no partial allocation leaks."""
        t1 = _make_tensor([1.0, 2.0])  # 8 bytes
        t2 = _make_tensor([3.0, 4.0, 5.0])  # 12 bytes
        pool = MemoryPoolManager(pool_size=12, device=torch.device("cpu"))

        views1 = pool.allocate_for_tensors([t1])
        assert torch.equal(views1[0], t1)

        # t2 doesn't fit in the remaining 4 bytes.
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_for_tensors([t2])

        # Pool should still be intact — t1's block is still valid.
        assert pool.has_block(_storage_ptr(t1))

    def test_atomic_allocation_failure(self):
        """When allocating multiple tensors atomically, if one doesn't fit,
        none should be allocated."""
        t1 = _make_tensor([1.0])  # 4 bytes
        t2 = _make_tensor([1.0] * 100)  # 400 bytes — won't fit
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))

        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_for_tensors([t1, t2])

        # Neither tensor should have been tracked.
        assert not pool.has_block(_storage_ptr(t1))
        assert not pool.has_block(_storage_ptr(t2))


# ---------------------------------------------------------------------------
# free_tensors
# ---------------------------------------------------------------------------


class TestFreeTensors:
    def test_free_and_reallocate(self):
        """After freeing, the space should be reusable."""
        t1 = _make_tensor([1.0, 2.0])  # 8 bytes
        pool = MemoryPoolManager(pool_size=8, device=torch.device("cpu"))

        pool.allocate_for_tensors([t1])
        assert pool.has_block(_storage_ptr(t1))

        pool.free_tensors([t1])
        assert not pool.has_block(_storage_ptr(t1))

        # Now a new tensor of the same size should fit.
        t2 = _make_tensor([3.0, 4.0])
        views = pool.allocate_for_tensors([t2])
        assert torch.equal(views[0], t2)

    def test_free_unknown_tensor_is_noop(self):
        """Freeing a tensor that was never allocated should not raise."""
        t = _make_tensor([1.0])
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        # Should not raise.
        pool.free_tensors([t])

    def test_free_multiple_tensors(self):
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0])
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))

        pool.allocate_for_tensors([t1])
        pool.allocate_for_tensors([t2])
        pool.free_tensors([t1, t2])

        assert not pool.has_block(_storage_ptr(t1))
        assert not pool.has_block(_storage_ptr(t2))

    def test_free_then_cross_call_reuse_is_broken(self):
        """After freeing, the same tensor should NOT get a cache hit — it
        should allocate a fresh block."""
        t = _make_tensor([1.0, 2.0])
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))

        pool.allocate_for_tensors([t])
        pool.free_tensors([t])
        assert not pool.has_block(_storage_ptr(t))

        # Re-allocate — should work (fresh allocation, not cache hit).
        views = pool.allocate_for_tensors([t])
        assert torch.equal(views[0], t)
        assert pool.has_block(_storage_ptr(t))

    def test_double_free_is_noop(self):
        """Freeing an already-freed tensor should not raise or corrupt state."""
        t = _make_tensor([1.0, 2.0])
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))

        pool.allocate_for_tensors([t])
        pool.free_tensors([t])
        # Second free — should be a no-op.
        pool.free_tensors([t])
        assert not pool.has_block(_storage_ptr(t))


# ---------------------------------------------------------------------------
# _allocate_multiple / _free_multiple — raw allocator
# ---------------------------------------------------------------------------


class TestRawAllocator:
    def test_allocate_single_block(self):
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        result = pool._allocate_multiple([50])

        assert result is not None
        assert len(result) == 1
        assert result[0].offset == 0
        assert result[0].size == 50

    def test_allocate_multiple_blocks(self):
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        result = pool._allocate_multiple([30, 40])

        assert result is not None
        assert len(result) == 2
        # Blocks should not overlap.
        offsets_ends = [(b.offset, b.offset + b.size) for b in result]
        offsets_ends.sort()
        for i in range(len(offsets_ends) - 1):
            assert offsets_ends[i][1] <= offsets_ends[i + 1][0]

    def test_allocate_exact_fit(self):
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        result = pool._allocate_multiple([100])

        assert result is not None
        assert result[0].offset == 0
        assert result[0].size == 100

    def test_allocate_fails_when_too_large(self):
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        result = pool._allocate_multiple([101])
        assert result is None

    def test_allocate_atomic_failure(self):
        """If total fits but fragmentation prevents placement, all fail."""
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        # Allocate 60, leaving 40 free.
        r1 = pool._allocate_multiple([60])
        assert r1 is not None

        # Try to allocate two blocks of 30 each (60 total > 40 remaining).
        r2 = pool._allocate_multiple([30, 30])
        assert r2 is None

        # The first allocation should still be intact.
        total_free = sum(b.size for b in pool._free_blocks)
        assert total_free == 40

    def test_free_and_merge(self):
        """Freeing adjacent blocks should merge them into one."""
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))

        r = pool._allocate_multiple([30, 30, 40])
        assert r is not None

        # Free all blocks.
        pool._free_multiple(r)

        # Should have merged back into a single free block.
        assert len(pool._free_blocks) == 1
        assert pool._free_blocks[0].offset == 0
        assert pool._free_blocks[0].size == 100

    def test_free_partial_merge(self):
        """Freeing non-adjacent blocks should not merge them."""
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        # Use equal sizes so allocation order matches request order
        # (largest-first sorting is stable for equal sizes).
        # Layout: r[0]=[0:25], r[1]=[25:50], r[2]=[50:75], r[3]=[75:100]
        r = pool._allocate_multiple([25, 25, 25, 25])
        assert r is not None

        # Free first and third, keep second and fourth.
        # Free blocks: [0:25] and [50:75] — not adjacent.
        pool._free_multiple([r[0], r[2]])

        # Should have 2 free blocks (not merged because r[1] sits between them).
        assert len(pool._free_blocks) == 2

    def test_invalid_allocation_raises(self):
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        with pytest.raises(ValueError):
            pool._allocate_multiple([])
        with pytest.raises(ValueError):
            pool._allocate_multiple([0])
        with pytest.raises(ValueError):
            pool._allocate_multiple([-1])

    def test_invalid_free_raises(self):
        pool = MemoryPoolManager(pool_size=100, device=torch.device("cpu"))
        with pytest.raises(ValueError):
            pool._free_multiple([])


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_tensor_list(self):
        """allocate_for_tensors with an empty list should return an empty list."""
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([])
        assert views == []

    def test_different_dtypes(self):
        """Tensors of different dtypes should each get their own block."""
        t_f32 = torch.tensor([1.0], dtype=torch.float32)
        t_f64 = torch.tensor([1.0], dtype=torch.float64)
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))

        views = pool.allocate_for_tensors([t_f32, t_f64])
        assert views[0].dtype == torch.float32
        assert views[1].dtype == torch.float64
        assert torch.equal(views[0], t_f32)
        assert torch.equal(views[1], t_f64)

    def test_view_with_storage_offset(self):
        """A tensor view with non-zero storage offset should be correctly
        mapped to the pool."""
        base = _make_tensor([1.0, 2.0, 3.0, 4.0, 5.0])
        view = base[2:4]  # [3.0, 4.0], storage_offset = 2

        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        views = pool.allocate_for_tensors([view])

        assert torch.equal(views[0], view)
        assert views[0].shape == (2,)

    def test_multidimensional_tensor_shape_preserved(self):
        """Multi-dimensional tensor shapes should be preserved in pool views."""
        t = torch.tensor([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))

        views = pool.allocate_for_tensors([t])
        assert views[0].shape == (3, 2)
        assert torch.equal(views[0], t)

    def test_allocate_multiple_preserves_request_order(self):
        """_allocate_multiple should return blocks in the same order as the
        input sizes, even though it allocates largest-first internally."""
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        # Sizes in non-sorted order.
        sizes = [10, 50, 20, 40]
        result = pool._allocate_multiple(sizes)

        assert result is not None
        # Each result block should match the requested size, in order.
        for i, size in enumerate(sizes):
            assert result[i].size == size


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
