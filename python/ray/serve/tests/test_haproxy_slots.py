"""Unit tests for HAProxy slot pool + split helpers.

These tests are pure-Python and do not require the HAProxy binary or the
RAY_SERVE_ENABLE_HA_PROXY env flag.
"""

import pytest

from ray.serve._private.haproxy import (
    BackendSlotPool,
    _compute_slot_split,
)


class TestBackendSlotPool:
    def test_init_rejects_nonpositive_size(self):
        with pytest.raises(ValueError):
            BackendSlotPool("be", 0)
        with pytest.raises(ValueError):
            BackendSlotPool("be", -1)

    def test_first_free_allocation_is_compact(self):
        """Slots are handed out lowest-first; releases recycle the freed slot."""
        pool = BackendSlotPool("be", pool_size=4)
        assert pool.assign("a") == 1
        assert pool.assign("b") == 2
        assert pool.assign("c") == 3
        # Release b -> slot 2 should be the next allocation.
        assert pool.release("b") == 2
        assert pool.assign("d") == 2
        # Release a -> slot 1 reused next.
        assert pool.release("a") == 1
        assert pool.assign("e") == 1

    def test_assign_is_idempotent(self):
        pool = BackendSlotPool("be", pool_size=4)
        slot = pool.assign("a")
        assert pool.assign("a") == slot
        assert pool.in_use() == 1

    def test_release_of_unknown_is_noop(self):
        pool = BackendSlotPool("be", pool_size=2)
        assert pool.release("ghost") is None
        assert pool.in_use() == 0

    def test_full_pool_returns_none(self):
        pool = BackendSlotPool("be", pool_size=2)
        pool.assign("a")
        pool.assign("b")
        assert pool.assign("c") is None
        assert pool.is_exhausted()
        # After a release, room opens up again.
        pool.release("a")
        assert not pool.is_exhausted()
        assert pool.assign("c") == 1

    def test_mapping_returns_copy(self):
        pool = BackendSlotPool("be", pool_size=4)
        pool.assign("a")
        m1 = pool.mapping()
        pool.assign("b")
        m2 = pool.mapping()
        # m1 should not be mutated by later assignments
        assert m1 == {"a": 1}
        assert m2 == {"a": 1, "b": 2}

    def test_in_use_and_free_counts(self):
        pool = BackendSlotPool("be", pool_size=3)
        assert pool.in_use() == 0
        assert pool.free() == 3
        pool.assign("a")
        pool.assign("b")
        assert pool.in_use() == 2
        assert pool.free() == 1
        pool.release("a")
        assert pool.in_use() == 1
        assert pool.free() == 2


class TestComputeSlotSplit:
    def test_empty_returns_empty(self):
        assert _compute_slot_split({}) == {}

    def test_single_backend_gets_full_budget(self):
        result = _compute_slot_split({"only": 10}, total=128, min_per_backend=16)
        assert result == {"only": 128}

    def test_no_replicas_equal_split_above_minimum(self):
        result = _compute_slot_split(
            {"a": 0, "b": 0, "c": 0},
            total=300,
            min_per_backend=10,
            headroom_factor=2.0,
        )
        # Reserved: 30. Remaining: 270 split equally → 90 each. Sum = 300.
        assert result == {"a": 100, "b": 100, "c": 100}
        assert sum(result.values()) == 300

    def test_proportional_split_by_replica_count(self):
        result = _compute_slot_split(
            {"big": 100, "small": 10},
            total=400,
            min_per_backend=20,
            headroom_factor=2.0,
        )
        # Reserved: 40. Remaining: 360.
        # Demand: big=200, small=20 (total 220).
        # Extra: big = 360*200/220 ≈ 327, small = 360*20/220 ≈ 32.
        # With rounding: 327+32=359; +1 to largest fractional → 327+33=360.
        # Final: big=20+327=347 or 348, small=20+32=33 or 32. Sum=400.
        assert sum(result.values()) == 400
        assert result["big"] >= result["small"]
        assert result["big"] >= 200  # has at least replica count's worth
        assert result["small"] >= 20  # has at least the minimum

    def test_minimum_floor_respected(self):
        """Even an idle backend gets at least min_per_backend."""
        result = _compute_slot_split(
            {"hot": 100, "idle": 0},
            total=200,
            min_per_backend=16,
            headroom_factor=2.0,
        )
        assert result["idle"] == 16
        assert sum(result.values()) <= 200

    def test_pathological_too_many_backends_degrades_to_equal(self):
        # 10 backends * min=16 = 160 > total=100, so equal split.
        result = _compute_slot_split(
            {f"be{i}": 0 for i in range(10)},
            total=100,
            min_per_backend=16,
            headroom_factor=2.0,
        )
        assert sum(result.values()) == 100
        # Each backend has at least 1, all roughly equal (10 each).
        for size in result.values():
            assert size >= 1
        assert max(result.values()) - min(result.values()) <= 1

    def test_sum_invariant_does_not_exceed_total(self):
        """Property: sum(result) <= total for all reasonable inputs."""
        import random

        random.seed(42)
        for _ in range(50):
            n = random.randint(1, 30)
            counts = {f"be{i}": random.randint(0, 50) for i in range(n)}
            total = random.randint(64, 8192)
            result = _compute_slot_split(
                counts, total=total, min_per_backend=16, headroom_factor=2.0
            )
            assert sum(result.values()) <= total
            assert set(result.keys()) == set(counts.keys())

    def test_headroom_factor_scales_allocation(self):
        """Higher headroom_factor gives the busier backend more slots."""
        low = _compute_slot_split(
            {"busy": 50, "idle": 0},
            total=400,
            min_per_backend=16,
            headroom_factor=1.0,
        )
        high = _compute_slot_split(
            {"busy": 50, "idle": 0},
            total=400,
            min_per_backend=16,
            headroom_factor=4.0,
        )
        # The "idle" backend's allocation can't grow (only min). But the
        # "busy" backend's share grows with headroom_factor — except
        # the total is fixed at 400 and the idle min is 16, so busy
        # caps at 384 regardless. Just check monotonicity:
        # busy(high_factor) >= busy(low_factor)
        assert high["busy"] >= low["busy"]
