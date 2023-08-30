#!/usr/bin/env python

import os
import tempfile
import unittest
import gymnasium as gym

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
        with tempfile.TemporaryDirectory() as tmpdir:
            config = (
                APPOConfig().environment("CartPole-v1").rollouts(enable_connectors=True)
            )
            algo = config.build()
            algo.train()
            result = algo.save(checkpoint_dir=tmpdir)

            path_to_checkpoint = os.path.join(
                result.checkpoint.path, "policies", "default_policy"
            )

            policy = Policy.from_checkpoint(path_to_checkpoint)

            self.assertIsNotNone(policy)

            # Add this policy to an Algorithm.
            algo = APPOConfig().framework(framework="torch").build("CartPole-v0")

            # Add the entire policy.
            self.assertIsNotNone(algo.add_policy("test_policy", policy=policy))

            # Add the same policy, but using individual parameter API.
            self.assertIsNotNone(
                algo.add_policy(
                    "test_policy_2",
                    policy_cls=type(policy),
                    observation_space=policy.observation_space,
                    action_space=policy.action_space,
                    config=policy.config,
                    policy_state=policy.get_state(),
                )
            )

    def test_restore_checkpoint_with_nested_obs_space(self):
        from ray.rllib.algorithms.ppo.ppo import PPOConfig

        obs_space = gym.spaces.Box(low=0, high=1, shape=(4,))
        # create 10 levels of nested observation space
        space = obs_space
        for i in range(10):
            space.original_space = gym.spaces.Discrete(2)
            space = space.original_space

        # TODO(Artur): Construct a PPO policy here without the algorithm once we are
        #  able to do that with RLModules.
        policy = (
            PPOConfig()
            .environment(
                observation_space=obs_space, action_space=gym.spaces.Discrete(2)
            )
            # Note (Artur): We have to choose num_rollout_workers=0 here, because
            # otherwise RolloutWorker will be health-checked without an env which
            # raises an error. You could also disable the health-check here.
            .rollouts(num_rollout_workers=0)
            .build()
            .get_policy()
        )

        ckpt_dir = "/tmp/test_ckpt"
        policy.export_checkpoint(ckpt_dir)

        # Create a new policy from the checkpoint.
        new_policy = Policy.from_checkpoint(ckpt_dir)

        # check that the new policy has the same nested observation space
        space = new_policy.observation_space
        for i in range(10):
            self.assertEqual(space.original_space, gym.spaces.Discrete(2))
            space = space.original_space


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
