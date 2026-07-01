"""Regression test for NaN agent-step entries in synchronous_parallel_sample.

A MultiAgentEnv with ``count_steps_by="agent_steps"`` leaves NaN entries in the
per-agent ``NUM_AGENT_STEPS_SAMPLED`` metric for agents that previously stepped
but are absent from the current batch. ``synchronous_parallel_sample`` calls
``int()`` on every per-agent value to advance its stopping criterion, which used
to raise ``ValueError: cannot convert float NaN to integer``. See #62635.
"""

import math
import unittest

from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_SAMPLED


class _FakeEnvRunner:
    """Local EnvRunner stub returning a fixed sample batch and metrics dict."""

    def __init__(self, agent_steps):
        self._agent_steps = agent_steps

    def sample(self, **kwargs):
        return SampleBatch()

    def get_metrics(self):
        return {NUM_AGENT_STEPS_SAMPLED: dict(self._agent_steps)}


class _FakeEnvRunnerGroup:
    """Local-only EnvRunnerGroup stub (no remote workers)."""

    def __init__(self, agent_steps):
        self.local_env_runner = _FakeEnvRunner(agent_steps)

    def num_remote_workers(self):
        return 0


def _collect(agent_steps):
    _, stats_dicts = synchronous_parallel_sample(
        worker_set=_FakeEnvRunnerGroup(agent_steps),
        max_agent_steps=1,
        concat=False,
        _return_metrics=True,
    )
    return stats_dicts


class TestNanAgentSteps(unittest.TestCase):
    def test_all_agents_active(self):
        # No agent is absent, so no NaN entries: every step is counted.
        stats = _collect({"agent_0": 3, "agent_1": 7})
        self.assertEqual(len(stats), 1)
        self.assertEqual(
            stats[0][NUM_AGENT_STEPS_SAMPLED], {"agent_0": 3, "agent_1": 7}
        )

    def test_some_agents_inactive(self):
        # agent_1 was active before but is absent now, leaving a NaN entry.
        # This used to raise ValueError; it must now complete and skip the NaN.
        agent_steps = {"agent_0": 4, "agent_1": float("nan"), "agent_2": 6}
        stats = _collect(agent_steps)
        self.assertEqual(len(stats), 1)
        returned = stats[0][NUM_AGENT_STEPS_SAMPLED]
        active = sum(int(v) for v in returned.values() if not math.isnan(float(v)))
        self.assertEqual(active, 10)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
