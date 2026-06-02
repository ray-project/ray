"""Regression test for NaN agent-step entries in synchronous_parallel_sample."""
import math
import unittest

_NUM_AGENT_STEPS_SAMPLED = "num_agent_steps_sampled"


def _count_agent_steps(stats_dicts):
    # Mirrors the patched generator in rllib/execution/rollout_ops.py.
    return sum(
        int(agent_stat)
        for stat_dict in stats_dicts
        for agent_stat in stat_dict[_NUM_AGENT_STEPS_SAMPLED].values()
        if not math.isnan(float(agent_stat))
    )


class TestNanAgentSteps(unittest.TestCase):
    def test_skips_nan_entries(self):
        stats = [{_NUM_AGENT_STEPS_SAMPLED: {"a": 3, "b": float("nan"), "c": 7}}]
        self.assertEqual(_count_agent_steps(stats), 10)

    def test_clean_stats_unchanged(self):
        stats = [{_NUM_AGENT_STEPS_SAMPLED: {"a": 10, "b": 20}}]
        self.assertEqual(_count_agent_steps(stats), 30)


if __name__ == "__main__":
    unittest.main()
