from functools import partial
import unittest

import gymnasium as gym

import ray
from ray import tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.examples.envs.classes.simple_corridor import SimpleCorridor


class TestSingleAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

        tune.register_env(
            "tune-registered",
            lambda cfg: SimpleCorridor({"corridor_length": 10}),
        )

        gym.register(
            "TestEnv-v0",
            partial(
                _gym_env_creator,
                env_context={"corridor_length": 10},
                env_descriptor=SimpleCorridor,
            ),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sample(self):
        config = (
            AlgorithmConfig().environment("CartPole-v1")
            # Vectorize x2 and by default, rollout 64 timesteps per individual env.
            .env_runners(num_envs_per_env_runner=2, rollout_fragment_length=64)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Expect error if both num_timesteps and num_episodes given.
        self.assertRaises(
            AssertionError,
            lambda: env_runner.sample(
                num_timesteps=10, num_episodes=10, random_actions=True
            ),
        )

        # Sample 10 episodes (5 per env) 100 times.
        for _ in range(100):
            episodes = env_runner.sample(num_episodes=10, random_actions=True)
            self.assertTrue(len(episodes) == 10)
            # Since we sampled complete episodes, there should be no ongoing episodes
            # being returned.
            self.assertTrue(all(e.is_done for e in episodes))

        # Sample 10 timesteps (5 per env) 100 times.
        for _ in range(100):
            episodes = env_runner.sample(num_timesteps=10, random_actions=True)
            # Check, whether the sum of lengths of all episodes returned is 20
            self.assertTrue(sum(len(e) for e in episodes) == 10)

        # Sample (by default setting: rollout_fragment_length=64) 10 times.
        for _ in range(100):
            episodes = env_runner.sample(random_actions=True)
            # Check, whether the sum of lengths of all episodes returned is 128
            # 2 (num_env_per_worker) * 64 (rollout_fragment_length).
            self.assertTrue(sum(len(e) for e in episodes) == 128)

    def test_async_vector_env(self):
        """Tests, whether SingleAgentGymEnvRunner can run with vector envs."""

        for env in ["TestEnv-v0", "CartPole-v1", SimpleCorridor, "tune-registered"]:
            config = (
                AlgorithmConfig().environment(env)
                # Vectorize x5 and by default, rollout 64 timesteps per individual env.
                .env_runners(
                    num_env_runners=0,
                    num_envs_per_env_runner=5,
                    rollout_fragment_length=10,
                    remote_worker_envs=True,
                )
            )

            env_runner = SingleAgentEnvRunner(config=config)

            # Sample with the async-vectorized env.
            episodes = env_runner.sample(random_actions=True)
            # Assert length of all fragments is  `rollout_fragment_length`.
            self.assertEqual(
                sum(len(e) for e in episodes),
                config.num_envs_per_env_runner * config.rollout_fragment_length,
            )
            env_runner.stop()

    def test_distributed_env_runner(self):
        """Tests, whether SingleAgentGymEnvRunner can be distributed."""

        remote_class = ray.remote(num_cpus=1, num_gpus=0)(SingleAgentEnvRunner)

        # Test with both parallelized sub-envs and w/o.
        async_vectorization_mode = [False, True]

        for async_ in async_vectorization_mode:

            for env_spec in ["tune-registered", "CartPole-v1", SimpleCorridor]:
                config = (
                    AlgorithmConfig().environment(env_spec)
                    # Vectorize x5 and by default, rollout 64 timesteps per individual
                    # env.
                    .env_runners(
                        num_env_runners=5,
                        num_envs_per_env_runner=5,
                        rollout_fragment_length=10,
                        remote_worker_envs=async_,
                    )
                )
                array = [
                    remote_class.remote(config=config)
                    for _ in range(config.num_env_runners)
                ]
                # Sample in parallel.
                results = [a.sample.remote(random_actions=True) for a in array]
                results = ray.get(results)
                # Loop over individual EnvRunner Actor's results and inspect each.
                for episodes in results:
                    # Assert length of all fragments is  `rollout_fragment_length`.
                    self.assertEqual(
                        sum(len(e) for e in episodes),
                        config.num_envs_per_env_runner * config.rollout_fragment_length,
                    )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
