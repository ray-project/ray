import math

import pytest

from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_SAMPLED
from ray.rllib.utils.metrics.stats import SumStats


class _FakeEnvRunner:
    """Minimal local EnvRunner returning canned samples and metrics."""

    def __init__(self, stats):
        self._stats = stats

    def sample(self, **kwargs):
        # New-stack sampling returns a list of episodes; an empty list is fine
        # for this test - we only exercise the metrics/step-counting path.
        return []

    def get_metrics(self):
        return self._stats


class _FakeEnvRunnerGroup:
    """Local-only EnvRunnerGroup (no remote workers)."""

    def __init__(self, stats):
        self.local_env_runner = _FakeEnvRunner(stats)

    def num_remote_workers(self):
        return 0


def test_synchronous_parallel_sample_skips_nan_agent_steps():
    """NaN per-agent step counts must not crash step accounting.

    Regression test for #62635: an agent that stepped earlier in training but
    is absent from the currently sampled episodes keeps a `NaN`
    NUM_AGENT_STEPS_SAMPLED entry. `count_steps_by="agent_steps"` summed these
    via `int(stat)`, raising `ValueError: cannot convert float NaN to integer`.
    """
    present = SumStats()
    present.push(7)
    absent = SumStats()
    absent.push(float("nan"))
    stats = {NUM_AGENT_STEPS_SAMPLED: {"agent_0": present, "agent_1": absent}}

    # Sanity: the absent agent's stat really is NaN.
    assert math.isnan(float(absent))

    worker_set = _FakeEnvRunnerGroup(stats)

    # Must not raise; the NaN agent is skipped.
    _, all_stats = synchronous_parallel_sample(
        worker_set=worker_set,
        max_agent_steps=1,
        _uses_new_env_runners=True,
        _return_metrics=True,
    )
    assert all_stats == [stats]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
