#!/usr/bin/env python

import os
from pathlib import Path
import unittest

import ray
from ray.rllib.algorithms.appo.appo import APPOConfig

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy import Policy
from ray.rllib.utils.test_utils import framework_iterator


def _do_checkpoint_twice_test(framework):
    # Checks if we can load a policy from a checkpoint (at least) twice
    config = (
        PPOConfig().rollouts(num_rollout_workers=0).evaluation(evaluation_num_workers=0)
    )
    for fw in framework_iterator(config, frameworks=[framework]):
        algo1 = config.build(env="CartPole-v1")
        algo2 = config.build(env="Pendulum-v1")

        algo1.train()
        algo2.train()

        policy1 = algo1.get_policy()
        policy1.export_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_1")

        policy2 = algo2.get_policy()
        policy2.export_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_2")

        algo1.stop()
        algo2.stop()

        # Create two policies from different checkpoints
        Policy.from_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_1")
        Policy.from_checkpoint("/tmp/test_policy_from_checkpoint_twice_p_2")


class TestPolicyFromCheckpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_policy_from_checkpoint_twice_tf(self):
        return _do_checkpoint_twice_test("tf")

    def test_policy_from_checkpoint_twice_tf2(self):
        return _do_checkpoint_twice_test("tf2")

    def test_policy_from_checkpoint_twice_torch(self):
        return _do_checkpoint_twice_test("torch")

    def test_add_policy_connector_enabled(self):
        rllib_dir = Path(__file__).parent.parent.parent
        path_to_checkpoint = os.path.join(
            rllib_dir,
            "tests",
            "data",
            "checkpoints",
            "APPO_CartPole-v1-connector-enabled",
            "policies",
            "default_policy",
        )

        policy = Policy.from_checkpoint(path_to_checkpoint)

        self.assertIsNotNone(policy)

        # Add this policy to a trainer.
        trainer = APPOConfig().framework(framework="torch").build("CartPole-v0")

        # Add the entire policy.
        self.assertIsNotNone(trainer.add_policy("test_policy", policy=policy))

        # Add the same policy, but using individual parameter API.
        self.assertIsNotNone(
            trainer.add_policy(
                "test_policy_2",
                policy_cls=type(policy),
                observation_space=policy.observation_space,
                action_space=policy.action_space,
                config=policy.config,
                policy_state=policy.get_state(),
            )
        )


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
