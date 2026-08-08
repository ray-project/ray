"""Unit tests for MemoryPoolManager and packed-layout wire contract.
"""

import random
import sys

import pytest
import torch

from ray.experimental.rdt.nixl_memory_pool import (
    MemoryPoolManager,
    NixlOutOfMemoryError,
    packed_run_offsets,
    split_run_by_desc_lens,
)
from ray.experimental.rdt.nixl_tensor_transport import _merged_run_desc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tensor(values, dtype=torch.float32):
    """Create a contiguous CPU tensor."""
    return torch.tensor(values, dtype=dtype)


def _nbytes(t):
    return t.numel() * t.element_size()


def _layout(tensors):
    """Byte sizes and element-size alignments for a list of tensors."""
    return [_nbytes(t) for t in tensors], [t.element_size() for t in tensors]


# ---------------------------------------------------------------------------
# Wire-contract layout helpers
# ---------------------------------------------------------------------------


class TestPackedLayout:
    def test_single_size(self):
        offsets, extent = packed_run_offsets([12], [4])
        assert offsets == [0]
        assert extent == 12

    def test_uniform_dtype_has_no_padding(self):
        """A run sharing one dtype packs tightly, matching a contiguous buffer."""
        offsets, extent = packed_run_offsets([12, 8], [4, 4])
        assert offsets == [0, 12]
        assert extent == 20

    def test_alignment_padding_for_wider_dtype(self):
        # 1-byte tensor, then a float64 that must start on an 8-byte boundary.
        offsets, extent = packed_run_offsets([1, 8], [1, 8])
        assert offsets == [0, 8]
        assert extent == 16

    def test_already_aligned(self):
        offsets, extent = packed_run_offsets([16, 16], [8, 8])
        assert offsets == [0, 16]
        assert extent == 32

    def test_split_single_desc(self):
        sizes, aligns = [1, 8], [1, 8]
        _, extent = packed_run_offsets(sizes, aligns)
        runs = split_run_by_desc_lens(sizes, aligns, [extent])
        assert runs == [[0, 1]]

    def test_split_multiple_descs(self):
        sizes, aligns = [12, 8, 4], [4, 4, 4]
        _, e0 = packed_run_offsets(sizes[:2], aligns[:2])
        _, e1 = packed_run_offsets(sizes[2:], aligns[2:])
        runs = split_run_by_desc_lens(sizes, aligns, [e0, e1])
        assert runs == [[0, 1], [2]]

    def test_split_one_per_tensor(self):
        sizes, aligns = [12, 8, 4], [4, 4, 4]
        runs = split_run_by_desc_lens(sizes, aligns, sizes)
        assert runs == [[0], [1], [2]]

    def test_split_mismatch_raises(self):
        with pytest.raises(ValueError):
            split_run_by_desc_lens([12, 8], [4, 4], [12])
        with pytest.raises(ValueError):
            split_run_by_desc_lens([12], [4], [12, 8])
        with pytest.raises(ValueError):
            # 1 packs at offset 0; the float64 pads to 8, so extent is 16 not 15.
            split_run_by_desc_lens([1, 8], [1, 8], [15])

    def test_roundtrip_randomized(self):
        """split_run_by_desc_lens recovers exactly the packer's runs."""
        rng = random.Random(0)
        for _ in range(50):
            n = rng.randint(1, 12)
            aligns = [rng.choice([1, 2, 4, 8, 16]) for _ in range(n)]
            sizes = [a * rng.randint(1, 8) for a in aligns]
            # Simulate packing into random run breaks.
            cuts = sorted(rng.sample(range(1, n), k=rng.randint(0, min(3, n - 1))))
            cuts = [0] + cuts + [n]
            desc_lens = []
            expected_runs = []
            for a, b in zip(cuts, cuts[1:]):
                run = list(range(a, b))
                _, extent = packed_run_offsets(sizes[a:b], aligns[a:b])
                desc_lens.append(extent)
                expected_runs.append(run)
            recovered = split_run_by_desc_lens(sizes, aligns, desc_lens)
            assert recovered == expected_runs
            for run in recovered:
                offs, _ = packed_run_offsets(
                    [sizes[i] for i in run], [aligns[i] for i in run]
                )
                assert offs[0] == 0


# ---------------------------------------------------------------------------
# Receiver-side merge decision for user-supplied target buffers
# ---------------------------------------------------------------------------


class TestMergedRunDesc:
    """A run collapses to one descriptor only if the buffers already match it."""

    def _desc_for(self, buffers):
        sizes, aligns = _layout(buffers)
        offsets, extent = packed_run_offsets(sizes, aligns)
        run = list(range(len(buffers)))
        return _merged_run_desc(buffers, run, offsets, extent), extent

    def test_views_of_one_buffer_merge(self):
        parent = torch.zeros(6, dtype=torch.float32)
        desc, extent = self._desc_for([parent[0:2], parent[2:6]])
        assert desc is not None
        addr, length, _dev_id = desc
        assert addr == parent.data_ptr()
        assert length == extent == 24

    def test_single_buffer_merges(self):
        t = _make_tensor([1.0, 2.0])
        desc, extent = self._desc_for([t])
        assert desc == (t.data_ptr(), extent, 0)

    def test_separate_allocations_do_not_merge(self):
        desc, _ = self._desc_for([torch.zeros(2), torch.zeros(4)])
        assert desc is None

    def test_gap_between_views_does_not_merge(self):
        parent = torch.zeros(8, dtype=torch.float32)
        desc, _ = self._desc_for([parent[0:2], parent[3:7]])
        assert desc is None

    def test_out_of_order_views_do_not_merge(self):
        parent = torch.zeros(6, dtype=torch.float32)
        desc, _ = self._desc_for([parent[4:6], parent[0:4]])
        assert desc is None

    def test_mixed_dtype_naturally_aligned_views_merge(self):
        # torch itself requires the float64 view to start on an 8-byte boundary,
        # which is exactly where the sender packs it.
        parent = torch.zeros(24, dtype=torch.int8)
        desc, extent = self._desc_for([parent[0:3], parent[8:24].view(torch.float64)])
        assert desc is not None
        assert desc[0] == parent.data_ptr()
        assert desc[1] == extent == 24


# ---------------------------------------------------------------------------
# allocate_group — basic allocation and data copy
# ---------------------------------------------------------------------------


class TestAllocateGroup:
    def test_single_tensor(self):
        t = _make_tensor([1.0, 2.0, 3.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions, placements, views = pool.allocate_group("o1", [t])

        assert len(regions) == 1
        assert len(views) == 1
        assert torch.equal(views[0], t)
        assert placements[0] == (0, 0)
        assert regions[0].numel() == _nbytes(t)
        assert "o1" in pool._allocated_by_obj

    def test_multiple_independent_tensors_one_block(self):
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0, 5.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        regions, placements, views = pool.allocate_group("o1", [t1, t2])

        assert len(regions) == 1
        assert torch.equal(views[0], t1)
        assert torch.equal(views[1], t2)
        assert placements[0][0] == placements[1][0] == 0

    def test_pool_views_are_backed_by_pool_tensor(self):
        t = _make_tensor([10.0, 20.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        _regions, _placements, views = pool.allocate_group("o1", [t])

        assert (
            views[0].untyped_storage().data_ptr()
            == pool.get_pool_tensor().untyped_storage().data_ptr()
        )

    def test_data_is_copied_not_aliased(self):
        t = _make_tensor([1.0, 2.0, 3.0])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        _regions, _placements, views = pool.allocate_group("o1", [t])

        original = views[0].clone()
        t[0] = 999.0
        assert torch.equal(views[0], original)

    def test_view_of_large_storage_copies_only_view_bytes(self):
        """A small view of a large storage should only consume the view's bytes."""
        base = torch.arange(1000, dtype=torch.float32)
        view = base[100:104]  # 16 bytes
        # Pool sized just above the view — far smaller than the full storage.
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        regions, _placements, views = pool.allocate_group("o1", [view])

        assert regions[0].numel() == 16
        assert pool._allocated_by_obj["o1"][0].size >= 16
        assert torch.equal(views[0], view)
        # Full storage would not fit.
        assert base.untyped_storage().nbytes() > 64

    def test_mixed_dtypes_align(self):
        t_f32 = torch.tensor([1.0], dtype=torch.float32)  # 4 bytes
        t_f64 = torch.tensor([2.0], dtype=torch.float64)  # 8 bytes
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        _regions, placements, views = pool.allocate_group("o1", [t_f32, t_f64])

        assert torch.equal(views[0], t_f32)
        assert torch.equal(views[1], t_f64)
        assert views[0].dtype == torch.float32
        assert views[1].dtype == torch.float64
        # The float64 starts on an 8-byte boundary, so it is padded past the
        # float32's 4 bytes.
        assert placements[1][1] == 8

    def test_multidimensional_shape_preserved(self):
        t = torch.tensor([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        _regions, _placements, views = pool.allocate_group("o1", [t])
        assert views[0].shape == (3, 2)
        assert torch.equal(views[0], t)

    def test_view_with_storage_offset(self):
        base = _make_tensor([1.0, 2.0, 3.0, 4.0, 5.0])
        view = base[2:4]
        pool = MemoryPoolManager(pool_size=1024, device=torch.device("cpu"))
        _regions, _placements, views = pool.allocate_group("o1", [view])
        assert torch.equal(views[0], view)
        assert views[0].shape == (2,)

    def test_reallocate_same_obj_id_replaces_prior(self):
        pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
        t1 = _make_tensor([1.0, 2.0])
        t2 = _make_tensor([3.0, 4.0, 5.0, 6.0])
        pool.allocate_group("o1", [t1])
        assert len(pool._allocated_by_obj["o1"]) == 1
        regions, _placements, views = pool.allocate_group("o1", [t2])
        assert torch.equal(views[0], t2)
        assert regions[0].numel() == _nbytes(t2)
        # Only one allocation recorded for o1.
        assert len(pool._allocated_by_obj) == 1


# ---------------------------------------------------------------------------
# allocate_group — fragmentation / multi-block packing
# ---------------------------------------------------------------------------


class TestFragmentedPacking:
    def test_multi_block_on_fragmented_pool(self):
        """Allocate three blocks, free the outer two, then pack a group that
        cannot fit in either hole alone — should span both holes."""
        # Use sizes that are multiples of alignment so extents are exact.
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
        regions, placements, views = pool.allocate_group("g", [t0, t1])

        assert len(regions) == 2
        assert placements[0][0] != placements[1][0]
        assert torch.equal(views[0], t0)
        assert torch.equal(views[1], t1)

        # Wire-contract round-trip: desc lens recover the runs.
        sizes, aligns = _layout([t0, t1])
        desc_lens = [r.numel() for r in regions]
        runs = split_run_by_desc_lens(sizes, aligns, desc_lens)
        assert runs == [[0], [1]]


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

        _regions, _placements, views1 = pool.allocate_group("o1", [t1])
        assert torch.equal(views1[0], t1)

        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("o2", [t2])

        # Free and reallocate to confirm state is intact.
        assert pool.free_object("o1")
        _regions, _placements, views2 = pool.allocate_group("o2", [t2])
        assert torch.equal(views2[0], t2)
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
        regions1, _, views1 = pool.allocate_group("o1", [t_small])
        with pytest.raises(NixlOutOfMemoryError):
            pool.allocate_group("o1", [t_big])
        assert "o1" in pool._allocated_by_obj
        assert torch.equal(views1[0], t_small)
        assert regions1[0].numel() == _nbytes(t_small)

    def test_replace_can_reuse_own_space(self):
        """Re-extract for the same obj_id can reuse its previously allocated space."""
        pool = MemoryPoolManager(pool_size=32, device=torch.device("cpu"))
        t1 = torch.zeros(8, dtype=torch.float32)  # 32 bytes — fills the pool
        t2 = torch.arange(8, dtype=torch.float32)
        pool.allocate_group("o1", [t1])
        # Without reclaiming o1's own block this would OOM.
        _regions, _placements, views = pool.allocate_group("o1", [t2])
        assert torch.equal(views[0], t2)
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
        _regions, _placements, views = pool.allocate_group("o2", [t2])
        assert torch.equal(views[0], t2)

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
        _regions, _placements, views = pool.allocate_group("big", [t_big])
        assert views[0].shape == t_big.shape


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
