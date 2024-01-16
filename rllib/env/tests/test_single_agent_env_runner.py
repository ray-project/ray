import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner


class TestSingleAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sample(self):
        config = (
            AlgorithmConfig().environment("CartPole-v1")
            # Vectorize x2 and by default, rollout 64 timesteps per individual env.
            .rollouts(num_envs_per_worker=2, rollout_fragment_length=64)
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

    def test_distributed_env_runner(self):
        """Tests, whether SingleAgentGymEnvRunner can be distributed."""

        remote_class = ray.remote(num_cpus=1, num_gpus=0)(SingleAgentEnvRunner)

        # Test with both parallelized sub-envs and w/o.
        remote_worker_envs = [False, True]

        for envs_parallel in remote_worker_envs:
            config = (
                AlgorithmConfig().environment("CartPole-v1")
                # Vectorize x2 and by default, rollout 64 timesteps per individual env.
                .rollouts(
                    num_rollout_workers=5,
                    num_envs_per_worker=5,
                    rollout_fragment_length=10,
                    remote_worker_envs=envs_parallel,
                )
            )

            array = [
                remote_class.remote(config=config)
                for _ in range(config.num_rollout_workers)
            ]
            # Sample in parallel.
            results = [a.sample.remote(random_actions=True) for a in array]
            results = ray.get(results)
            # Loop over individual EnvRunner Actor's results and inspect each.
            for episodes in results:
                # Assert length of all fragments is  `rollout_fragment_length`.
                self.assertEqual(
                    sum(len(e) for e in episodes),
                    config.num_envs_per_worker * config.rollout_fragment_length,
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
