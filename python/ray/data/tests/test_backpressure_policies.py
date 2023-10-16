import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from pyarrow.hdfs import os

from ray.data._internal.execution.backpressure_policy import (
    ConcurrencyCapBackpressurePolicy,
)
from ray.data._internal.execution.streaming_executor_state import Topology


class TestConcurrentcyCapBackpressurePolicy(unittest.TestCase):
    @contextmanager
    def _patch_env_var(self, value):
        with patch.dict(
            os.environ, {ConcurrencyCapBackpressurePolicy.CONFIG_ENV_VAR: value}
        ):
            yield

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

        with self._patch_env_var(
            f"{init_cap},{cap_multiply_threshold},{cap_multiplier}"
        ):
            policy = ConcurrencyCapBackpressurePolicy(topology)

        self.assertEqual(policy._concurrency_caps[op], 4)
        # Gradually increase num_tasks_running to the cap.
        for i in range(1, init_cap + 1):
            self.assertTrue(policy.can_run(op))
            op.metrics.num_tasks_running = i
        # Now num_tasks_running reaches the cap, so can_run should return False.
        self.assertFalse(policy.can_run(op))

        # Increase num_tasks_finished to the threshold to trigger the cap increase,
        # and can_run should return True.
        op.metrics.num_tasks_finished = init_cap * cap_multiply_threshold
        self.assertEqual(policy.can_run(op), True)
        self.assertEqual(policy._concurrency_caps[op], init_cap * cap_multiplier)

    def test_env_var_config(self):
        topology = MagicMock(Topology)
        # Test good config.
        with self._patch_env_var("10,0.3,1.5"):
            policy = ConcurrencyCapBackpressurePolicy(topology)
            self.assertEqual(policy._init_cap, 10)
            self.assertEqual(policy._cap_multiply_threshold, 0.3)
            self.assertEqual(policy._cap_multiplier, 1.5)

        # Test bad configs.
        with self._patch_env_var("10,0.3"):
            with self.assertRaises(ValueError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_env_var("-1,0.3,1.5"):
            with self.assertRaises(ValueError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_env_var("10,1.1,1.5"):
            with self.assertRaises(ValueError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_env_var("10,0.3,0.5"):
            with self.assertRaises(ValueError):
                policy = ConcurrencyCapBackpressurePolicy(topology)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
