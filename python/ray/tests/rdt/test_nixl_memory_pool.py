"""Unit tests for MemoryPoolManager and packed-layout wire contract.
"""

import random
import sys

import pytest
import torch

from ray.experimental.rdt.nixl_memory_pool import (
    _MAX_ALIGNMENT,
    MemoryPoolManager,
    NixlOutOfMemoryError,
    TensorLayout,
    group_tensors_by_desc,
    packed_offsets,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tensor(values, dtype=torch.float32):
    """Create a contiguous CPU tensor."""
    return torch.tensor(values, dtype=dtype)


def _nbytes(t):
    return t.numel() * t.element_size()


def _layout(tensors):
    """Packed size and element-size alignment of each tensor, in tensor order."""
    return [TensorLayout(_nbytes(t), t.element_size()) for t in tensors]


def _unpack(regions, tensors):
    """Recover per-tensor views from packed regions, the way a receiver does.

    ``allocate_group`` returns only the regions to transfer, so what the pool
    packed is checked by decoding them with the same two layout functions the
    receive path uses.
    """
    layouts = _layout(tensors)
    desc_groups = group_tensors_by_desc(layouts, [r.numel() for r in regions])
    views = [None] * len(tensors)
    for region, desc_group in zip(regions, desc_groups):
        offsets, _ = packed_offsets([layouts[i] for i in desc_group])
        for offset, i in zip(offsets, desc_group):
            views[i] = (
                region[offset : offset + layouts[i].nbytes]
                .view(tensors[i].dtype)
                .reshape(tensors[i].shape)
            )
    return views


# ---------------------------------------------------------------------------
# Wire-contract layout helpers
# ---------------------------------------------------------------------------


class TestPackedLayout:
    def test_single_size(self):
        offsets, packed_nbytes = packed_offsets([TensorLayout(12, 4)])
        assert offsets == [0]
        assert packed_nbytes == 12

    def test_uniform_dtype_has_no_padding(self):
        """A group sharing one dtype packs tightly, matching a contiguous buffer."""
        offsets, packed_nbytes = packed_offsets(
            [TensorLayout(12, 4), TensorLayout(8, 4)]
        )
        assert offsets == [0, 12]
        assert packed_nbytes == 20

    def test_alignment_padding_for_wider_dtype(self):
        # 1-byte tensor, then a float64 that must start on an 8-byte boundary.
        offsets, packed_nbytes = packed_offsets(
            [TensorLayout(1, 1), TensorLayout(8, 8)]
        )
        assert offsets == [0, 8]
        assert packed_nbytes == 16

    def test_already_aligned(self):
        offsets, packed_nbytes = packed_offsets(
            [TensorLayout(16, 8), TensorLayout(16, 8)]
        )
        assert offsets == [0, 16]
        assert packed_nbytes == 32

    def test_split_single_desc(self):
        layouts = [TensorLayout(1, 1), TensorLayout(8, 8)]
        _, packed_nbytes = packed_offsets(layouts)
        desc_groups = group_tensors_by_desc(layouts, [packed_nbytes])
        assert desc_groups == [[0, 1]]

    def test_split_multiple_descs(self):
        layouts = [TensorLayout(12, 4), TensorLayout(8, 4), TensorLayout(4, 4)]
        _, nbytes0 = packed_offsets(layouts[:2])
        _, nbytes1 = packed_offsets(layouts[2:])
        desc_groups = group_tensors_by_desc(layouts, [nbytes0, nbytes1])
        assert desc_groups == [[0, 1], [2]]

    def test_split_one_per_tensor(self):
        layouts = [TensorLayout(12, 4), TensorLayout(8, 4), TensorLayout(4, 4)]
        desc_groups = group_tensors_by_desc(
            layouts, [layout.nbytes for layout in layouts]
        )
        assert desc_groups == [[0], [1], [2]]

    def test_split_mismatch_raises(self):
        with pytest.raises(ValueError):
            group_tensors_by_desc([TensorLayout(12, 4), TensorLayout(8, 4)], [12])
        with pytest.raises(ValueError):
            group_tensors_by_desc([TensorLayout(12, 4)], [12, 8])
        with pytest.raises(ValueError):
            # 1 packs at offset 0; the float64 pads to 8, so the packed byte
            # count is 16 not 15.
            group_tensors_by_desc([TensorLayout(1, 1), TensorLayout(8, 8)], [15])

    def test_roundtrip_randomized(self):
        """group_tensors_by_desc recovers exactly the packer's groups."""
        rng = random.Random(0)
        for _ in range(50):
            n = rng.randint(1, 12)
            aligns = [rng.choice([1, 2, 4, 8, 16]) for _ in range(n)]
            layouts = [TensorLayout(a * rng.randint(1, 8), a) for a in aligns]
            # Simulate packing into random group breaks.
            cuts = sorted(rng.sample(range(1, n), k=rng.randint(0, min(3, n - 1))))
            cuts = [0] + cuts + [n]
            packed_group_nbytes = []
            expected_desc_groups = []
            for a, b in zip(cuts, cuts[1:]):
                desc_group = list(range(a, b))
                _, packed_nbytes = packed_offsets(layouts[a:b])
                packed_group_nbytes.append(packed_nbytes)
                expected_desc_groups.append(desc_group)
            recovered = group_tensors_by_desc(layouts, packed_group_nbytes)
            assert recovered == expected_desc_groups
            for desc_group in recovered:
                offs, _ = packed_offsets([layouts[i] for i in desc_group])
                assert offs[0] == 0


# ---------------------------------------------------------------------------
# allocate_group — basic allocation and data copy
# ---------------------------------------------------------------------------


class TestAllocateGroup:
    def test_single_tensor(self):
        t = _make_tensor([1.0, 2.0, 3.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [t])

        assert len(regions) == 1
        assert regions[0].numel() == _nbytes(t)
        assert torch.equal(_unpack(regions, [t])[0], t)
        assert "o1" in pool._allocated_by_obj

    def test_multiple_independent_tensors_one_block(self):
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0, 5.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [t1, t2])
        views = _unpack(regions, [t1, t2])

        # Both tensors share a dtype, so they pack into one block with no pad.
        assert len(regions) == 1
        assert regions[0].numel() == _nbytes(t1) + _nbytes(t2)
        assert torch.equal(views[0], t1)
        assert torch.equal(views[1], t2)

    def test_regions_are_backed_by_pool_tensor(self):
        t = _make_tensor([10.0, 20.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [t])

        assert (
            regions[0].untyped_storage().data_ptr()
            == pool.get_pool_tensor().untyped_storage().data_ptr()
        )

    def test_data_is_copied_not_aliased(self):
        t = _make_tensor([1.0, 2.0, 3.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [t])
        view = _unpack(regions, [t])[0]

        original = view.clone()
        t[0] = 999.0
        assert torch.equal(view, original)

    def test_view_of_large_storage_copies_only_view_bytes(self):
        """A small view of a large storage should only consume the view's bytes."""
        base = torch.arange(1000, dtype=torch.float32)
        view = base[100:104]  # 16 bytes
        # Pool sized just above the view — far smaller than the full storage.
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [view])

        assert regions[0].numel() == 16
        assert torch.equal(_unpack(regions, [view])[0], view)
        # Full storage would not fit.
        assert base.untyped_storage().nbytes() > 64

    def test_mixed_dtypes_align(self):
        t_f32 = torch.tensor([1.0], dtype=torch.float32)  # 4 bytes
        t_f64 = torch.tensor([2.0], dtype=torch.float64)  # 8 bytes
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [t_f32, t_f64])
        views = _unpack(regions, [t_f32, t_f64])

        # The float64 starts on an 8-byte boundary, so the region carries 4
        # bytes of padding past the float32.
        assert regions[0].numel() == 16
        assert torch.equal(views[0], t_f32)
        assert torch.equal(views[1], t_f64)

    def test_multidimensional_tensor(self):
        t = torch.tensor([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [t])
        assert regions[0].numel() == _nbytes(t)
        assert torch.equal(_unpack(regions, [t])[0], t)

    def test_view_with_storage_offset(self):
        base = _make_tensor([1.0, 2.0, 3.0, 4.0, 5.0])
        view = base[2:4]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", [view])
        assert regions[0].numel() == _nbytes(view)
        assert torch.equal(_unpack(regions, [view])[0], view)

    def test_ordered_views_of_one_storage(self):
        """Ordered views of one weight pack into a single tight block."""
        base = torch.arange(64, dtype=torch.float32).reshape(8, 8)
        rows = [base[i] for i in range(8)]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", rows)

        # One consecutive group: a single block, tightly packed in order.
        assert len(regions) == 1
        assert regions[0].numel() == 8 * 32
        for row, view in zip(rows, _unpack(regions, rows)):
            assert torch.equal(view, row)

    def test_reversed_views_still_byte_exact(self):
        """Sources supplied out of storage order must still copy correctly."""
        base = torch.arange(32, dtype=torch.float32).reshape(4, 8)
        rows = [base[i] for i in reversed(range(4))]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", rows)

        for row, view in zip(rows, _unpack(regions, rows)):
            assert torch.equal(view, row)

    def test_interleaved_storages_still_byte_exact(self):
        """Tensors drawn from two separate storages pack correctly."""
        a = torch.arange(8, dtype=torch.float32)
        b = torch.arange(100, 108, dtype=torch.float32)
        tensors = [a[0:2], b[0:2], a[2:4], b[2:4]]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", tensors)

        for tensor, view in zip(tensors, _unpack(regions, tensors)):
            assert torch.equal(view, tensor)

    def test_mixed_dtypes_copied_correctly(self):
        """A dtype change mid-group pads the layout but keeps the bytes."""
        f32 = torch.arange(4, dtype=torch.float32)
        f64 = torch.arange(2, dtype=torch.float64)
        i8 = torch.arange(3, dtype=torch.int8)
        tensors = [f32, f64, i8, f32 + 1]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions = pool.allocate_group("o1", tensors)

        for tensor, view in zip(tensors, _unpack(regions, tensors)):
            assert torch.equal(view, tensor)

    def test_gap_in_source_not_copied(self):
        """Bytes skipped between two source views must not land in the pool."""
        base = torch.arange(16, dtype=torch.float32)
        tensors = [base[0:4], base[8:12]]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        views = _unpack(pool.allocate_group("o1", tensors), tensors)

        assert torch.equal(views[0], base[0:4])
        assert torch.equal(views[1], base[8:12])

    def test_reallocate_same_obj_id_replaces_prior(self):
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0, 5.0, 6.0])
        pool.allocate_group("o1", [t1])
        assert len(pool._allocated_by_obj["o1"]) == 1
        regions = pool.allocate_group("o1", [t2])
        assert regions[0].numel() == _nbytes(t2)
        assert torch.equal(_unpack(regions, [t2])[0], t2)
        # Only one allocation recorded for o1.
        assert len(pool._allocated_by_obj) == 1


# ---------------------------------------------------------------------------
# allocate_group — fragmentation / multi-block packing
# ---------------------------------------------------------------------------


class TestFragmentedPacking:
    def test_multi_block_on_fragmented_pool(self):
        """Allocate three blocks, free the outer two, then pack a group that
        cannot fit in either hole alone — should span both holes."""
        # Use sizes that are multiples of alignment so byte counts are exact.
        # Each "filler" is 32 bytes; holes after freeing outer two are 32 each.
        pool = MemoryPoolManager(pool_size=96, device=torch.device("cpu"))
        fillers = [
            torch.zeros(8, dtype=torch.float32),  # 32 bytes
            torch.zeros(8, dtype=torch.float32),
            torch.zeros(8, dtype=torch.float32),
        ]
        pool.allocate_group("f0", [fillers[0]])
        pool.allocate_group("f1", [fillers[1]])
        pool.allocate_group("f2", [fillers[2]])
        assert pool.free_object("f0")
        assert pool.free_object("f2")
        # Middle block still allocated; free list has two 32-byte holes.
        # Request two 32-byte tensors — neither hole fits both, so 2 blocks.
        t0 = torch.arange(8, dtype=torch.float32)
        t1 = torch.arange(8, dtype=torch.float32) + 10
        regions = pool.allocate_group("g", [t0, t1])
        views = _unpack(regions, [t0, t1])

        assert len(regions) == 2
        assert torch.equal(views[0], t0)
        assert torch.equal(views[1], t1)

        # Wire-contract round-trip: region byte counts recover one group per block.
        desc_groups = group_tensors_by_desc(
            _layout([t0, t1]), [r.numel() for r in regions]
        )
        assert desc_groups == [[0], [1]]


# ---------------------------------------------------------------------------
# allocate_group — OOM
# ---------------------------------------------------------------------------


class TestOOM:
    def test_oom_single_tensor(self):
        t = _make_tensor([1.0, 2.0, 3.0])  # 12 bytes
        pool = MemoryPoolManager(pool_size=4, device=torch.device("cpu"))

        with pytest.raises(NixlOutOfMemoryError, match="out of memory"):
            pool.allocate_group("o1", [t])

    def test_oom_does_not_corrupt_pool_state(self):
        t1 = _make_tensor([1.0, 2.0])  # 8 bytes
        t2 = _make_tensor([3.0, 4.0, 5.0])  # 12 bytes
        pool = MemoryPoolManager(pool_size=16, device=torch.device("cpu"))

        regions = pool.allocate_group("o1", [t1])
        assert torch.equal(_unpack(regions, [t1])[0], t1)

        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("o2", [t2])

        # Free and reallocate to confirm state is intact.
        assert pool.free_object("o1")
        regions = pool.allocate_group("o2", [t2])
        assert torch.equal(_unpack(regions, [t2])[0], t2)
        assert pool.free_object("o2")

    def test_atomic_allocation_failure(self):
        """When the group cannot be fully packed, no partial state is committed."""
        t1 = _make_tensor([1.0])  # 4 bytes
        t2 = _make_tensor([1.0] * 100)  # 400 bytes
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        free_before = [(b.offset, b.size) for b in pool._free_blocks]

        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("o1", [t1, t2])

        free_after = [(b.offset, b.size) for b in pool._free_blocks]
        assert free_before == free_after
        assert "o1" not in pool._allocated_by_obj

    def test_oom_on_replace_keeps_prior_allocation(self):
        """A failed re-extract for the same obj_id must not free the prior blocks."""
        pool = MemoryPoolManager(pool_size=32, device=torch.device("cpu"))
        t_small = _make_tensor([1.0, 2.0])  # 8 bytes
        # Bigger than the whole pool even after reclaiming the prior block.
        t_big = _make_tensor([1.0] * 20)  # 80 bytes
        regions = pool.allocate_group("o1", [t_small])
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("o1", [t_big])
        assert "o1" in pool._allocated_by_obj
        assert regions[0].numel() == _nbytes(t_small)
        assert torch.equal(_unpack(regions, [t_small])[0], t_small)

    def test_replace_can_reuse_own_space(self):
        """Re-extract for the same obj_id can reuse its previously allocated space."""
        pool = MemoryPoolManager(pool_size=32, device=torch.device("cpu"))
        t1 = torch.zeros(8, dtype=torch.float32)  # 32 bytes — fills the pool
        t2 = torch.arange(8, dtype=torch.float32)
        pool.allocate_group("o1", [t1])
        # Without reclaiming o1's own block this would OOM.
        regions = pool.allocate_group("o1", [t2])
        assert torch.equal(_unpack(regions, [t2])[0], t2)
        assert len(pool._allocated_by_obj) == 1


# ---------------------------------------------------------------------------
# free_object
# ---------------------------------------------------------------------------


class TestFreeObject:
    def test_free_and_reallocate(self):
        t1 = _make_tensor([1.0, 2.0])  # 8 bytes
        pool = MemoryPoolManager(pool_size=32, device=torch.device("cpu"))

        pool.allocate_group("o1", [t1])
        assert pool.free_object("o1")
        assert "o1" not in pool._allocated_by_obj

        t2 = _make_tensor([3.0, 4.0])
        regions = pool.allocate_group("o2", [t2])
        assert torch.equal(_unpack(regions, [t2])[0], t2)

    def test_free_unknown_is_noop(self):
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        assert not pool.free_object("missing")

    def test_block_merging(self):
        """After freeing adjacent blocks, merged space is usable for a larger alloc."""
        t1 = torch.zeros(8, dtype=torch.float32)  # 32 bytes
        t2 = torch.zeros(8, dtype=torch.float32)
        t3 = torch.zeros(8, dtype=torch.float32)
        pool = MemoryPoolManager(pool_size=96, device=torch.device("cpu"))

        pool.allocate_group("o1", [t1])
        pool.allocate_group("o2", [t2])
        pool.allocate_group("o3", [t3])

        t_big = torch.zeros(16, dtype=torch.float32)  # 64 bytes

        assert pool.free_object("o1")
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("big", [t_big])

        assert pool.free_object("o2")
        regions = pool.allocate_group("big", [t_big])
        assert regions[0].numel() == _nbytes(t_big)


# ---------------------------------------------------------------------------
# Receive side: allocate_regions / copy_out_and_free / free_blocks
# ---------------------------------------------------------------------------


class TestAllocateRegions:
    def test_regions_are_exact_size_and_backed_by_pool(self):
        pool = MemoryPoolManager(pool_size=128, device=torch.device("cpu"))
        regions, blocks = pool.allocate_regions([10, 20])

        assert [r.numel() for r in regions] == [10, 20]
        assert len(blocks) == 2
        for region in regions:
            assert region.dtype == torch.uint8
            assert (
                region.untyped_storage().data_ptr()
                == pool.get_pool_tensor().untyped_storage().data_ptr()
            )

    def test_blocks_are_aligned_for_any_dtype(self):
        """Unaligned region sizes must still leave later blocks viewable."""
        pool = MemoryPoolManager(pool_size=256, device=torch.device("cpu"))
        _, blocks = pool.allocate_regions([1, 3, 7, 9])
        for block in blocks:
            assert block.offset % _MAX_ALIGNMENT == 0

    def test_round_trips_a_sender_packed_group(self):
        """A region sized like a sender descriptor decodes back to the tensors.

        This is the wire contract the receive path relies on: the pool only
        needs to hand back a correctly aligned region of the right length.
        """
        tensors = [
            _make_tensor([1], dtype=torch.int8),
            _make_tensor([2.0, 3.0, 4.0]),
            _make_tensor([5.0, 6.0], dtype=torch.float64),
        ]
        layouts = _layout(tensors)
        _, packed_nbytes = packed_offsets(layouts)

        pool = MemoryPoolManager(pool_size=256, device=torch.device("cpu"))
        # Occupy the front of the pool so the region is not trivially at
        # offset 0, the way a receive into a used pool would land.
        pool.allocate_regions([1])
        regions, _ = pool.allocate_regions([packed_nbytes])

        views = _unpack(regions, tensors)
        for view, tensor in zip(views, tensors):
            view.copy_(tensor)
            assert torch.equal(view, tensor)

    def test_atomic_allocation_failure(self):
        pool = MemoryPoolManager(pool_size=16, device=torch.device("cpu"))
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_regions([8, 12])
        # Pool state unchanged: the whole pool is still allocatable.
        regions, blocks = pool.allocate_regions([16])
        assert regions[0].numel() == 16
        pool.free_blocks(blocks)
        assert sum(b.size for b in pool._free_blocks) == 16

    def test_oom_on_fragmentation(self):
        """A region must be contiguous, so split free space cannot serve it."""
        pool = MemoryPoolManager(pool_size=48, device=torch.device("cpu"))
        _, first = pool.allocate_regions([16])
        _, middle = pool.allocate_regions([16])
        _, last = pool.allocate_regions([16])
        pool.free_blocks(first + last)

        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_regions([32])
        assert sum(b.size for b in pool._free_blocks) == 32

        pool.free_blocks(middle)
        regions, _ = pool.allocate_regions([32])
        assert regions[0].numel() == 32


class TestCopyOutAndFree:
    def test_copies_are_independent_of_pool(self):
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        regions, blocks = pool.allocate_regions([12])
        view = regions[0].view(torch.float32)
        view.copy_(_make_tensor([1.0, 2.0, 3.0]))

        copies = pool.copy_out_and_free([view], blocks)

        assert torch.equal(copies[0], _make_tensor([1.0, 2.0, 3.0]))
        assert (
            copies[0].untyped_storage().data_ptr()
            != pool.get_pool_tensor().untyped_storage().data_ptr()
        )

        # Reusing the block must not disturb the copy.
        reused, _ = pool.allocate_regions([12])
        reused[0].fill_(99)
        assert torch.equal(copies[0], _make_tensor([1.0, 2.0, 3.0]))

    def test_blocks_are_reusable_without_gc(self):
        """Blocks come back immediately, so sequential receives can exceed the
        pool size in aggregate."""
        pool = MemoryPoolManager(pool_size=12, device=torch.device("cpu"))
        for _ in range(3):
            regions, blocks = pool.allocate_regions([12])
            pool.copy_out_and_free([regions[0].view(torch.float32)], blocks)

        assert sum(b.size for b in pool._free_blocks) == 12

    def test_frees_blocks_even_if_copy_fails(self):
        """A failed copy out must not strand the blocks it was handed."""
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        regions, blocks = pool.allocate_regions([16])

        with pytest.raises(RuntimeError):
            pool.copy_out_and_free(regions, blocks, target_device="not_a_device")

        assert sum(b.size for b in pool._free_blocks) == 64

    def test_free_blocks_is_noop_for_empty_list(self):
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        pool.free_blocks([])
        assert sum(b.size for b in pool._free_blocks) == 64


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
