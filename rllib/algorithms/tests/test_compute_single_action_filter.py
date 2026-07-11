import unittest

import numpy as np

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID


class _RecordingFilter:
    """Wraps an observation filter and records how it is called."""

    def __init__(self, inner):
        self._inner = inner
        self.calls = []

    def __call__(self, x, *args, **kwargs):
        self.calls.append(kwargs.get("update", None))
        return self._inner(x, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class TestComputeSingleActionFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_compute_single_action_applies_obs_filter(self):
        """`compute_single_action` must apply the observation filter.

        Regression test for #64087: it previously skipped the observation filter
        (e.g. `MeanStdFilter`) that `compute_actions` applies, silently returning
        actions computed from unfiltered observations.
        """
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )
            .environment("CartPole-v1")
            .env_runners(observation_filter="MeanStdFilter", num_env_runners=0)
        )
        algo = config.build()
        try:
            worker = algo.env_runner_group.local_env_runner
            recorder = _RecordingFilter(worker.filters[DEFAULT_POLICY_ID])
            worker.filters[DEFAULT_POLICY_ID] = recorder

            obs = np.array([0.1, -0.2, 0.05, 0.3], dtype=np.float32)
            algo.compute_single_action(obs, explore=False)

            # The filter must have been applied exactly once, in eval mode
            # (update=False), matching `compute_actions`.
            self.assertEqual(len(recorder.calls), 1)
            self.assertEqual(recorder.calls[0], False)
        finally:
            algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
