import functools
import time
import unittest
from collections import defaultdict
from contextlib import contextmanager
from unittest.mock import MagicMock

import ray
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
)


@contextmanager
def enable_backpressure_policies(policies):
    data_context = ray.data.DataContext.get_current()
    old_policies = data_context.get_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
        [],
    )
    data_context.set_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
        policies,
    )
    yield
    data_context.set_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
        old_policies,
    )


class TestConcurrencyCapBackpressurePolicy(unittest.TestCase):
    """Tests for ConcurrencyCapBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        cls._cluster_cpus = 10
        ray.init(num_cpus=cls._cluster_cpus)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    @contextmanager
    def _patch_config(self, init_cap, cap_multiply_threshold, cap_multiplier):
        data_context = ray.data.DataContext.get_current()
        data_context.set_config(
            ConcurrencyCapBackpressurePolicy.INIT_CAP_CONFIG_KEY,
            init_cap,
        )
        data_context.set_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLY_THRESHOLD_CONFIG_KEY,
            cap_multiply_threshold,
        )
        data_context.set_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLIER_CONFIG_KEY,
            cap_multiplier,
        )
        yield
        data_context.remove_config(ConcurrencyCapBackpressurePolicy.INIT_CAP_CONFIG_KEY)
        data_context.remove_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLY_THRESHOLD_CONFIG_KEY
        )
        data_context.remove_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLIER_CONFIG_KEY
        )

    def test_basic(self):
        op = MagicMock()
        op.metrics = MagicMock(
            num_tasks_running=0,
            num_tasks_finished=0,
        )
        topology = {op: MagicMock()}

        init_cap = 4
        cap_multiply_threshold = 0.5
        cap_multiplier = 2.0

        with self._patch_config(init_cap, cap_multiply_threshold, cap_multiplier):
            policy = ConcurrencyCapBackpressurePolicy(topology)

        self.assertEqual(policy._concurrency_caps[op], 4)
        # Gradually increase num_tasks_running to the cap.
        for i in range(1, init_cap + 1):
            self.assertTrue(policy.can_run(op))
            op.metrics.num_tasks_running = i
        # Now num_tasks_running reaches the cap, so can_run should return False.
        self.assertFalse(policy.can_run(op))

        # If we increase num_task_finished to the threshold (4 * 0.5 = 2),
        # it should trigger the cap to increase.
        op.metrics.num_tasks_finished = init_cap * cap_multiply_threshold
        self.assertEqual(policy.can_run(op), True)
        self.assertEqual(policy._concurrency_caps[op], init_cap * cap_multiplier)

        # Now the cap is 8 (4 * 2).
        # If we increase num_tasks_finished directly to the next-level's threshold
        # (8 * 2 * 0.5 = 8), it should trigger the cap to increase twice.
        op.metrics.num_tasks_finished = (
            policy._concurrency_caps[op] * cap_multiplier * cap_multiply_threshold
        )
        op.metrics.num_tasks_running = 0
        self.assertEqual(policy.can_run(op), True)
        self.assertEqual(policy._concurrency_caps[op], init_cap * cap_multiplier**3)

    def test_config(self):
        topology = {}
        # Test good config.
        with self._patch_config(10, 0.3, 1.5):
            policy = ConcurrencyCapBackpressurePolicy(topology)
            self.assertEqual(policy._init_cap, 10)
            self.assertEqual(policy._cap_multiply_threshold, 0.3)
            self.assertEqual(policy._cap_multiplier, 1.5)

        # Test bad configs.
        with self._patch_config(-1, 0.3, 1.5):
            with self.assertRaises(AssertionError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_config(10, 1.1, 1.5):
            with self.assertRaises(AssertionError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_config(10, 0.3, 0.5):
            with self.assertRaises(AssertionError):
                policy = ConcurrencyCapBackpressurePolicy(topology)

    def test_e2e(self):
        """A simple E2E test with ConcurrencyCapBackpressurePolicy enabled."""

        @ray.remote(num_cpus=0)
        class RecordTimeActor:
            def __init__(self):
                self._start_time = defaultdict(lambda: float("inf"))
                self._end_time = defaultdict(lambda: 0.0)

            def record_start_time(self, index):
                self._start_time[index] = min(time.time(), self._start_time[index])

            def record_end_time(self, index):
                self._end_time[index] = max(time.time(), self._end_time[index])

            def get_start_and_end_time(self, index):
                return self._start_time[index], self._end_time[index]

        actor = RecordTimeActor.remote()

        def map_func(data, index):
            actor.record_start_time.remote(index)
            yield data
            actor.record_end_time.remote(index)

        with enable_backpressure_policies([ConcurrencyCapBackpressurePolicy]):
            # Creat a dataset with 2 map ops. Each map op has N tasks, where N is
            # the number of cluster CPUs.
            N = self.__class__._cluster_cpus
            ds = ray.data.range(N, parallelism=N)
            # Use different `num_cpus` to make sure they don't fuse.
            ds = ds.map_batches(
                functools.partial(map_func, index=1), batch_size=None, num_cpus=1
            )
            ds = ds.map_batches(
                functools.partial(map_func, index=2), batch_size=None, num_cpus=1.1
            )
            res = ds.take_all()
            self.assertEqual(len(res), N)

            # We recorded the start and end time of each op,
            # check that these 2 ops are executed interleavingly.
            # This means that the executor didn't allocate all resources to the first
            # op in the beginning.
            start1, end1 = ray.get(actor.get_start_and_end_time.remote(1))
            start2, end2 = ray.get(actor.get_start_and_end_time.remote(2))
            assert start1 < start2 < end1 < end2, (start1, start2, end1, end2)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
