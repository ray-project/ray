import gym
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_running_specific_algo_with_generic_config(self):
        """Tests, whether some algo can be run with the generic AlgorithmConfig."""
        config = (
            AlgorithmConfig(algo_class=PPO)
            .environment("CartPole-v0")
            .training(lr=0.12345, train_batch_size=3000)
        )
        algo = config.build()
        self.assertTrue(algo.config.lr == 0.12345)
        self.assertTrue(algo.config.train_batch_size == 3000)
        algo.train()
        algo.stop()

    def test_freezing_of_algo_config(self):
        """Tests, whether freezing an AlgorithmConfig actually works as expected."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v0")
            .training(lr=0.12345, train_batch_size=3000)
            .multi_agent(
                policies={
                    "pol1": (None, None, None, AlgorithmConfig.overrides(lr=0.001))
                },
                policy_mapping_fn=lambda agent_id, episode, worker, **kw: "pol1",
            )
        )
        config.freeze()

        def set_lr(config):
            config.lr = 0.01

        self.assertRaisesRegex(
            AttributeError,
            "Cannot set attribute.+of an already frozen AlgorithmConfig",
            lambda: set_lr(config),
        )

        # TODO: Figure out, whether we should convert all nested structures into
        #  frozen ones (set -> frozenset; dict -> frozendict; list -> tuple).

        def set_one_policy(config):
            config.policies["pol1"] = (None, None, None, {"lr": 0.123})

        # self.assertRaisesRegex(
        #    AttributeError,
        #    "Cannot set attribute.+of an already frozen AlgorithmConfig",
        #    lambda: set_one_policy(config),
        # )

    def test_rollout_fragment_length(self):
        """Tests the proper auto-computation of the `rollout_fragment_length`."""
        config = (
            AlgorithmConfig()
            .rollouts(
                num_rollout_workers=4,
                num_envs_per_worker=3,
                rollout_fragment_length="auto",
            )
            .training(train_batch_size=2456)
        )
        # 2456 / 3 * 4 -> 204.666 -> 204 or 205 (depending on worker index).
        # Actual train batch size: 2454 (off by only 2)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=0) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=1) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=2) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=3) == 204)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=4) == 204)

        config = (
            AlgorithmConfig()
            .rollouts(
                num_rollout_workers=3,
                num_envs_per_worker=2,
                rollout_fragment_length="auto",
            )
            .training(train_batch_size=4000)
        )
        # 4000 / 6 -> 666.66 -> 666 or 667 (depending on worker index)
        # Actual train batch size: 4000 (perfect match)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=0) == 667)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=1) == 667)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=2) == 667)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=3) == 666)

        config = (
            AlgorithmConfig()
            .rollouts(
                num_rollout_workers=12,
                rollout_fragment_length="auto",
            )
            .training(train_batch_size=1342)
        )
        # 1342 / 12 -> 111.83 -> 111 or 112 (depending on worker index)
        # Actual train batch size: 1342 (perfect match)
        for i in range(11):
            self.assertTrue(config.get_rollout_fragment_length(worker_index=i) == 112)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=11) == 111)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=12) == 111)

    def test_detect_atari_env(self):
        """Tests that we can properly detect Atari envs."""
        config = AlgorithmConfig().environment(
            env="ALE/Breakout-v5", env_config={"frameskip": 1}
        )
        config.validate()
        self.assertTrue(config.is_atari)

        config = AlgorithmConfig().environment(env="ALE/Pong-v5")
        config.validate()
        self.assertTrue(config.is_atari)

        config = AlgorithmConfig().environment(env="CartPole-v1")
        config.validate()
        # We do not auto-detect callable env makers for Atari envs.
        self.assertFalse(config.is_atari)

        config = AlgorithmConfig().environment(
            env=lambda ctx: gym.make(
                "GymV26Environment-v0",
                env_id="ALE/Breakout-v5",
                make_kwargs={"frameskip": 1},
            )
        )
        config.validate()
        # We do not auto-detect callable env makers for Atari envs.
        self.assertFalse(config.is_atari)

        config = AlgorithmConfig().environment(env="NotAtari")
        config.validate()
        self.assertFalse(config.is_atari)

    def test_rl_module_api(self):
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .framework("torch")
            .rollouts(enable_connectors=True)
            .rl_module(_enable_rl_module_api=True)
        )

        config.validate()
        self.assertEqual(config.rl_module_class, PPOTorchRLModule)

        class A:
            pass

        config = config.rl_module(rl_module_class=A)
        config.validate()
        self.assertEqual(config.rl_module_class, A)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
