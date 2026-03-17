"""Tests for MemoryWatermarkBackpressurePolicy.

Covers:
- Hysteresis state machine (NORMAL → THROTTLED → NORMAL)
- GCS query caching / TTL
- Exception handling in _query_utilization (returns cached, not 0.0)
- max_task_output_bytes_to_read always returns None (deadlock prevention)
- Validation of watermark thresholds
- DataContext field integration
- Thread safety
- Policy registration and disable via set_config
- E2E test: no deadlock under simulated memory pressure
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES,
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    get_backpressure_policies,
)
from ray.data._internal.execution.backpressure_policy.memory_watermark_backpressure_policy import (  # noqa: E501
    MemoryWatermarkBackpressurePolicy,
)
from ray.data.context import DataContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_policy(
    high=0.85,
    low=0.65,
    data_context=None,
):
    """Create a MemoryWatermarkBackpressurePolicy with sensible mocks."""
    if data_context is None:
        data_context = MagicMock(spec=DataContext)
        data_context.memory_watermark_high = high
        data_context.memory_watermark_low = low
    topology = {}
    resource_manager = MagicMock()
    return MemoryWatermarkBackpressurePolicy(
        data_context, topology, resource_manager
    )


def _patch_ray_resources(total, available):
    """Return a pair of patch context managers for ray.cluster_resources and
    ray.available_resources."""
    cluster_patch = patch(
        "ray.cluster_resources",
        return_value={"object_store_memory": total},
    )
    avail_patch = patch(
        "ray.available_resources",
        return_value={"object_store_memory": available},
    )
    return cluster_patch, avail_patch


# ---------------------------------------------------------------------------
# Unit Tests — no Ray cluster required
# ---------------------------------------------------------------------------


class TestMemoryWatermarkBasic(unittest.TestCase):
    """Core unit tests for the watermark state machine."""

    def test_initial_state_is_not_throttled(self):
        policy = _make_policy()
        op = MagicMock()
        # With default cached utilization = 0.0 (< HIGH), should be True.
        self.assertTrue(policy.can_add_input(op))
        self.assertFalse(policy._throttled)

    def test_throttle_engages_at_high_watermark(self):
        """Utilization >= HIGH should trigger throttle."""
        policy = _make_policy(high=0.80, low=0.60)
        op = MagicMock()

        # Simulate 85% utilisation (total=100, avail=15).
        cluster_p, avail_p = _patch_ray_resources(100.0, 15.0)
        with cluster_p, avail_p:
            # Force cache expiry.
            policy._last_query_time = 0.0
            result = policy.can_add_input(op)
        self.assertFalse(result)
        self.assertTrue(policy._throttled)

    def test_throttle_releases_below_low_watermark(self):
        """Utilization < LOW should release throttle."""
        policy = _make_policy(high=0.80, low=0.60)
        op = MagicMock()

        # First engage throttle.
        policy._throttled = True
        policy._cached_utilization = 0.90

        # Now simulate 50% utilisation (< LOW=0.60).
        cluster_p, avail_p = _patch_ray_resources(100.0, 50.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            result = policy.can_add_input(op)
        self.assertTrue(result)
        self.assertFalse(policy._throttled)

    def test_hysteresis_stays_throttled_between_low_and_high(self):
        """When throttled and util is between LOW and HIGH, should stay throttled."""
        policy = _make_policy(high=0.80, low=0.60)
        op = MagicMock()

        # Engage throttle.
        policy._throttled = True
        policy._cached_utilization = 0.85

        # Simulate 70% utilisation (LOW < 0.70 < HIGH).
        cluster_p, avail_p = _patch_ray_resources(100.0, 30.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            result = policy.can_add_input(op)
        self.assertFalse(result)
        self.assertTrue(policy._throttled)

    def test_hysteresis_prevents_cycling(self):
        """Oscillating utilisation between LOW and HIGH should not cycle throttle."""
        policy = _make_policy(high=0.85, low=0.65)
        op = MagicMock()

        # Engage throttle at 90%.
        cluster_p, avail_p = _patch_ray_resources(100.0, 10.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            policy.can_add_input(op)
        self.assertTrue(policy._throttled)

        # Drop to 70% — still between LOW(0.65) and HIGH(0.85) — stays throttled.
        cluster_p, avail_p = _patch_ray_resources(100.0, 30.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            result = policy.can_add_input(op)
        self.assertFalse(result)
        self.assertTrue(policy._throttled)

        # Rise to 80% — still between LOW and HIGH — stays throttled.
        cluster_p, avail_p = _patch_ray_resources(100.0, 20.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            result = policy.can_add_input(op)
        self.assertFalse(result)
        self.assertTrue(policy._throttled)

        # Drop to 60% (< LOW=0.65) — release.
        cluster_p, avail_p = _patch_ray_resources(100.0, 40.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            result = policy.can_add_input(op)
        self.assertTrue(result)
        self.assertFalse(policy._throttled)


class TestMaxTaskOutputBytesToRead(unittest.TestCase):
    """max_task_output_bytes_to_read must ALWAYS return None."""

    def test_returns_none_when_normal(self):
        policy = _make_policy()
        self.assertIsNone(policy.max_task_output_bytes_to_read(MagicMock()))

    def test_returns_none_when_throttled(self):
        """Even when throttled, output reading must not be restricted."""
        policy = _make_policy()
        policy._throttled = True
        self.assertIsNone(policy.max_task_output_bytes_to_read(MagicMock()))


class TestQueryUtilizationCaching(unittest.TestCase):
    """GCS query caching behaviour."""

    def test_cached_value_returned_within_ttl(self):
        policy = _make_policy()
        policy._cached_utilization = 0.42
        policy._last_query_time = time.monotonic()  # just cached

        # Should return cached value without calling ray.
        util = policy._query_utilization()
        self.assertAlmostEqual(util, 0.42)

    def test_fresh_query_after_ttl(self):
        policy = _make_policy()
        policy._cached_utilization = 0.42
        policy._last_query_time = 0.0  # expired

        cluster_p, avail_p = _patch_ray_resources(200.0, 40.0)
        with cluster_p, avail_p:
            util = policy._query_utilization()
        # 1.0 - 40/200 = 0.80
        self.assertAlmostEqual(util, 0.80)
        self.assertAlmostEqual(policy._cached_utilization, 0.80)

    def test_exception_returns_cached_not_zero(self):
        """GCS failure should return cached value, not 0.0 (false safety)."""
        policy = _make_policy()
        policy._cached_utilization = 0.75
        policy._last_query_time = 0.0

        with patch("ray.cluster_resources", side_effect=RuntimeError("GCS down")):
            util = policy._query_utilization()
        self.assertAlmostEqual(util, 0.75)

    def test_zero_total_returns_cached(self):
        """When total object_store_memory is 0, return cached (Ray not ready)."""
        policy = _make_policy()
        policy._cached_utilization = 0.30
        policy._last_query_time = 0.0

        cluster_p, avail_p = _patch_ray_resources(0.0, 0.0)
        with cluster_p, avail_p:
            util = policy._query_utilization()
        self.assertAlmostEqual(util, 0.30)

    def test_utilization_clamped_to_0_1(self):
        """Utilization should be clamped to [0.0, 1.0]."""
        policy = _make_policy()
        policy._last_query_time = 0.0

        # available > total (edge case during reporting lag)
        cluster_p, avail_p = _patch_ray_resources(100.0, 150.0)
        with cluster_p, avail_p:
            util = policy._query_utilization()
        self.assertAlmostEqual(util, 0.0)  # clamped to 0

    def test_n_operators_trigger_at_most_1_rpc(self):
        """Multiple can_add_input calls in one scheduling step should reuse cache."""
        policy = _make_policy(high=0.99, low=0.01)  # won't trigger

        # Prime the cache.
        cluster_p, avail_p = _patch_ray_resources(100.0, 50.0)
        with cluster_p, avail_p:
            policy._last_query_time = 0.0
            policy.can_add_input(MagicMock())

        # Now call again within TTL — should not query ray again.
        with patch("ray.cluster_resources") as mock_cluster:
            with patch("ray.available_resources") as mock_avail:
                for _ in range(10):
                    policy.can_add_input(MagicMock())
                mock_cluster.assert_not_called()
                mock_avail.assert_not_called()


class TestWatermarkValidation(unittest.TestCase):
    """Threshold validation at construction time."""

    def test_low_equal_high_raises(self):
        with self.assertRaises(ValueError):
            _make_policy(high=0.5, low=0.5)

    def test_low_greater_than_high_raises(self):
        with self.assertRaises(ValueError):
            _make_policy(high=0.5, low=0.7)

    def test_high_equal_1_raises(self):
        with self.assertRaises(ValueError):
            _make_policy(high=1.0, low=0.5)

    def test_low_equal_0_raises(self):
        with self.assertRaises(ValueError):
            _make_policy(high=0.8, low=0.0)

    def test_negative_low_raises(self):
        with self.assertRaises(ValueError):
            _make_policy(high=0.8, low=-0.1)


class TestDataContextIntegration(unittest.TestCase):
    """DataContext field and env-var integration."""

    def test_reads_from_data_context_fields(self):
        ctx = MagicMock(spec=DataContext)
        ctx.memory_watermark_high = 0.75
        ctx.memory_watermark_low = 0.55
        policy = MemoryWatermarkBackpressurePolicy(
            ctx, {}, MagicMock()
        )
        self.assertAlmostEqual(policy._high_watermark, 0.75)
        self.assertAlmostEqual(policy._low_watermark, 0.55)

    def test_falls_back_to_class_defaults(self):
        """When DataContext doesn't have watermark attrs, use defaults."""
        ctx = MagicMock(spec=[])  # empty spec — no attrs
        policy = MemoryWatermarkBackpressurePolicy(
            ctx, {}, MagicMock()
        )
        self.assertAlmostEqual(
            policy._high_watermark,
            MemoryWatermarkBackpressurePolicy.DEFAULT_HIGH_WATERMARK,
        )
        self.assertAlmostEqual(
            policy._low_watermark,
            MemoryWatermarkBackpressurePolicy.DEFAULT_LOW_WATERMARK,
        )

    def test_name_property(self):
        policy = _make_policy()
        self.assertEqual(policy.name, "MemoryWatermark")


class TestThreadSafety(unittest.TestCase):
    """Thread safety of _throttled state."""

    def test_concurrent_can_add_input(self):
        """Multiple threads calling can_add_input should not corrupt state."""
        policy = _make_policy(high=0.80, low=0.60)
        op = MagicMock()
        errors = []

        def worker(util_values):
            try:
                for total, avail in util_values:
                    cluster_p, avail_p = _patch_ray_resources(total, avail)
                    with cluster_p, avail_p:
                        policy._last_query_time = 0.0
                        policy.can_add_input(op)
            except Exception as e:
                errors.append(e)

        # Simulate oscillating utilisation from multiple threads.
        vals_a = [(100.0, 10.0)] * 50 + [(100.0, 50.0)] * 50  # high then low
        vals_b = [(100.0, 50.0)] * 50 + [(100.0, 10.0)] * 50  # low then high

        t1 = threading.Thread(target=worker, args=(vals_a,))
        t2 = threading.Thread(target=worker, args=(vals_b,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(errors, [])
        # The final state should be consistent (either True or False).
        self.assertIsInstance(policy._throttled, bool)


class TestPolicyRegistration(unittest.TestCase):
    """Policy is registered first in ENABLED_BACKPRESSURE_POLICIES."""

    def test_registered_first(self):
        self.assertIs(
            ENABLED_BACKPRESSURE_POLICIES[0],
            MemoryWatermarkBackpressurePolicy,
        )

    def test_in_enabled_list(self):
        self.assertIn(
            MemoryWatermarkBackpressurePolicy,
            ENABLED_BACKPRESSURE_POLICIES,
        )


# ---------------------------------------------------------------------------
# E2E Tests — require a Ray cluster
# ---------------------------------------------------------------------------


class TestMemoryWatermarkE2E(unittest.TestCase):
    """End-to-end tests with a real (local) Ray cluster."""

    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_disable_via_set_config(self):
        """Users can exclude MemoryWatermark via DataContext.set_config."""
        from ray.data._internal.execution.backpressure_policy import (
            ConcurrencyCapBackpressurePolicy,
            DownstreamCapacityBackpressurePolicy,
            ResourceBudgetBackpressurePolicy,
        )

        ctx = DataContext.get_current()
        # Exclude MemoryWatermark from the list.
        ctx.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [
                ConcurrencyCapBackpressurePolicy,
                ResourceBudgetBackpressurePolicy,
                DownstreamCapacityBackpressurePolicy,
            ],
        )
        try:
            policies = get_backpressure_policies(ctx, {}, MagicMock())
            policy_types = [type(p) for p in policies]
            self.assertNotIn(MemoryWatermarkBackpressurePolicy, policy_types)
        finally:
            ctx.remove_config(ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY)

    @patch(
        "ray.data._internal.execution.backpressure_policy."
        "memory_watermark_backpressure_policy.MemoryWatermarkBackpressurePolicy."
        "_query_utilization"
    )
    def test_no_deadlock_under_pressure(self, mock_query):
        """Pipeline should complete even when watermark is throttled.

        The ``ensure_liveness`` mechanism in ``streaming_executor_state.py``
        guarantees that at least one operator can run when all are
        backpressured and the topology is idle.
        """
        # Simulate 90% utilisation throughout.
        mock_query.return_value = 0.90

        ctx = DataContext.get_current()
        ctx.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [MemoryWatermarkBackpressurePolicy],
        )
        try:
            ds = ray.data.range(10, override_num_blocks=2)
            result = ds.map_batches(
                lambda batch: batch, batch_size=None
            ).take_all()
            self.assertEqual(len(result), 10)
        finally:
            ctx.remove_config(ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY)

    @patch(
        "ray.data._internal.execution.backpressure_policy."
        "memory_watermark_backpressure_policy.MemoryWatermarkBackpressurePolicy."
        "_query_utilization"
    )
    def test_blocks_new_tasks_not_output(self, mock_query):
        """Verify max_task_output_bytes_to_read returns None even when throttled."""
        mock_query.return_value = 0.90

        ctx = DataContext.get_current()
        ctx.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [MemoryWatermarkBackpressurePolicy],
        )
        try:
            policies = get_backpressure_policies(ctx, {}, MagicMock())
            watermark_policy = policies[0]
            self.assertIsInstance(watermark_policy, MemoryWatermarkBackpressurePolicy)

            op = MagicMock()
            # Trigger throttle.
            watermark_policy.can_add_input(op)
            self.assertTrue(watermark_policy._throttled)

            # Output reading must remain unrestricted.
            self.assertIsNone(watermark_policy.max_task_output_bytes_to_read(op))
        finally:
            ctx.remove_config(ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
