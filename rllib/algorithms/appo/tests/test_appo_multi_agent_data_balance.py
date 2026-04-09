"""Test how APPO handles per-policy data imbalance in multi-agent setups.

Note: PPO will always use "equalize" data across policies. So each policy will train on the same amount of data.

When a policy_mapping_fn maps more agents to one policy than another, the resulting
MultiAgentBatch has unequal per-policy data. This test verifies:
1. Default APPO (no minibatch_size): policies train on unequal amounts of data.
2. With minibatch_size set: MiniBatchCyclicIterator equalizes per-policy batch sizes.
"""

import unittest

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import NUM_MODULE_STEPS_TRAINED

NUM_AGENTS = 5


def policy_mapping_fn(agent_id, episode, **kw):
    return "policy_a" if agent_id in (0, 1, 2, 3) else "policy_b"


class TestAPPOMultiAgentDataBalance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def _build_config(self, *, minibatch_size=None, num_epochs=1):
        config = (
            APPOConfig()
            .environment(MultiAgentCartPole, env_config={"num_agents": NUM_AGENTS})
            .multi_agent(
                policies={"policy_a", "policy_b"},
                policy_mapping_fn=policy_mapping_fn,
            )
            .training(
                train_batch_size_per_learner=500,
                **(
                    {"minibatch_size": minibatch_size, "num_epochs": num_epochs}
                    if minibatch_size
                    else {}
                ),
            )
            .learners(num_learners=0)
            .env_runners(num_env_runners=1)
        )
        return config

    def _get_per_policy_steps(self, results):
        learner_results = results["learners"]
        return {
            pid: learner_results.get(pid, {}).get(NUM_MODULE_STEPS_TRAINED, 0)
            for pid in ["policy_a", "policy_b"]
        }

    def test_default_appo_unequal_data(self):
        """Without minibatch_size, policy_a trains on more data than policy_b."""
        algo = self._build_config().build()
        try:
            for _ in range(3):
                results = algo.train()
                steps = self._get_per_policy_steps(results)
                if steps["policy_a"] > 0 and steps["policy_b"] > 0:
                    self.assertGreater(
                        steps["policy_a"] / steps["policy_b"],
                        1.5,
                        "Expected policy_a to train on more data than policy_b "
                        "with biased policy mapping and no minibatch_size.",
                    )
        finally:
            algo.stop()

    def test_minibatch_equalizes_data(self):
        """With minibatch_size, both policies train on equal amounts of data."""
        algo = self._build_config(minibatch_size=50, num_epochs=4).build()
        try:
            for _ in range(3):
                results = algo.train()
                steps = self._get_per_policy_steps(results)
                if steps["policy_a"] > 0 and steps["policy_b"] > 0:
                    self.assertEqual(
                        steps["policy_a"],
                        steps["policy_b"],
                        "Expected equal per-policy training steps when "
                        "minibatch_size is set (MiniBatchCyclicIterator).",
                    )
        finally:
            algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
