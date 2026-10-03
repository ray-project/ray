import sys

import pytest

from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_SAMPLED
from ray.rllib.utils.metrics.stats.sum import SumStats


class _FakeLocalEnvRunner:
    def __init__(self):
        self._present_agent_steps = SumStats()
        self._present_agent_steps.push(2)
        # A known agent with no samples in the current metrics window has an
        # empty SumStats, whose compiled value is NaN.
        self._missing_agent_steps = SumStats()

    def sample(self, **kwargs):
        return ["sample"]

    def get_metrics(self):
        return {
            NUM_AGENT_STEPS_SAMPLED: {
                "present_agent": self._present_agent_steps,
                "missing_agent": self._missing_agent_steps,
            }
        }


class _FakeEnvRunnerGroup:
    def __init__(self):
        self.local_env_runner = _FakeLocalEnvRunner()

    def num_remote_workers(self):
        return 0


def test_synchronous_parallel_sample_ignores_nan_agent_step_metrics():
    samples, metrics = synchronous_parallel_sample(
        worker_set=_FakeEnvRunnerGroup(),
        max_agent_steps=2,
        concat=False,
        _uses_new_env_runners=True,
        _return_metrics=True,
    )

    assert samples == [["sample"]]
    assert len(metrics) == 1
    assert int(metrics[0][NUM_AGENT_STEPS_SAMPLED]["present_agent"]) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
