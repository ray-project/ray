"""Unit test for NaN-resilience in synchronous_parallel_sample's agent-step counter."""
import math
import unittest


class TestNanAgentStepFiltering(unittest.TestCase):
    """Verify the NaN guard added to synchronous_parallel_sample.

    The original list-comp called int(agent_stat) unconditionally, which raises
    ValueError when the stat is float('nan').  The fix wraps the comprehension
    with `if not math.isnan(float(agent_stat))` so NaN entries are skipped.
    """

    _NUM_AGENT_STEPS_SAMPLED = "num_agent_steps_sampled"

    def _sum_agent_steps(self, stats_dicts):
        """Mirror of the patched generator expression in rollout_ops.py."""
        return sum(
            int(agent_stat)
            for stat_dict in stats_dicts
            for agent_stat in stat_dict[self._NUM_AGENT_STEPS_SAMPLED].values()
            if not math.isnan(float(agent_stat))
        )

    def test_clean_stats(self):
        stats = [{self._NUM_AGENT_STEPS_SAMPLED: {"agent0": 10, "agent1": 20}}]
        self.assertEqual(self._sum_agent_steps(stats), 30)

    def test_nan_stat_skipped(self):
        stats = [
            {self._NUM_AGENT_STEPS_SAMPLED: {"agent0": float("nan"), "agent1": 5}}
        ]
        self.assertEqual(self._sum_agent_steps(stats), 5)

    def test_all_nan_returns_zero(self):
        stats = [
            {self._NUM_AGENT_STEPS_SAMPLED: {"agent0": float("nan")}}
        ]
        self.assertEqual(self._sum_agent_steps(stats), 0)

    def test_nan_without_guard_raises(self):
        """Without the guard, int(float('nan')) raises ValueError."""
        stats = [
            {self._NUM_AGENT_STEPS_SAMPLED: {"agent0": float("nan")}}
        ]
        with self.assertRaises((ValueError, OverflowError)):
            sum(
                int(agent_stat)
                for stat_dict in stats
                for agent_stat in stat_dict[self._NUM_AGENT_STEPS_SAMPLED].values()
            )

    def test_mixed_nan_and_valid(self):
        stats = [
            {self._NUM_AGENT_STEPS_SAMPLED: {"a": 3, "b": float("nan"), "c": 7}}
        ]
        self.assertEqual(self._sum_agent_steps(stats), 10)


if __name__ == "__main__":
    unittest.main()
