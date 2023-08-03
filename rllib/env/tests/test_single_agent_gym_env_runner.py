import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.testing.single_agent_gym_env_runner import SingleAgentGymEnvRunner


class TestSingleAgentGymEnvRunner(unittest.TestCase):
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
        env_runner = SingleAgentGymEnvRunner(config=config)

        # Expect error if both num_timesteps and num_episodes given.
        self.assertRaises(
            AssertionError, lambda: env_runner.sample(num_timesteps=10, num_episodes=10)
        )

        # Sample 10 episodes (5 per env) 100 times.
        for _ in range(100):
            done_episodes, ongoing_episodes = env_runner.sample(num_episodes=10)
            self.assertTrue(len(done_episodes + ongoing_episodes) == 10)
            # Since we sampled complete episodes, there should be no ongoing episodes
            # being returned.
            assert len(ongoing_episodes) == 0
            # Check, whether all done_episodes returned are indeed terminated.
            self.assertTrue(all(e.is_done for e in done_episodes))

        # Sample 10 timesteps (5 per env) 100 times.
        for _ in range(100):
            done_episodes, ongoing_episodes = env_runner.sample(num_timesteps=10)
            # Check, whether all done_episodes returned are indeed terminated.
            self.assertTrue(all(e.is_done for e in done_episodes))
            # Check, whether all done_episodes returned are indeed terminated.
            self.assertTrue(not any(e.is_done for e in ongoing_episodes))

        # Sample (by default setting: rollout_fragment_length=64) 10 times.
        for _ in range(100):
            done_episodes, ongoing_episodes = env_runner.sample()
            # Check, whether all done_episodes returned are indeed terminated.
            self.assertTrue(all(e.is_done for e in done_episodes))
            # Check, whether all done_episodes returned are indeed terminated.
            self.assertTrue(not any(e.is_done for e in ongoing_episodes))

    def test_distributed_env_runner(self):
        """Tests, whether SingleAgentGymEnvRunner can be distributed."""

        remote_class = ray.remote(num_cpus=1, num_gpus=0)(SingleAgentGymEnvRunner)

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
            results = [a.sample.remote() for a in array]
            results = ray.get(results)
            # Loop over individual EnvRunner Actor's results and inspect each.
            for result in results:
                # SingleAgentGymEnvRunners return tuples: (completed eps, ongoing eps).
                completed, ongoing = result
                # Make sure all completed Episodes are indeed done.
                self.assertTrue(all(e.is_done for e in completed))
                # Same for ongoing ones (make sure they are not done).
                self.assertTrue(not any(e.is_done for e in ongoing))
                # Assert length of all fragments is  `rollout_fragment_length`.
                self.assertEqual(
                    sum(len(e) for e in completed + ongoing),
                    config.num_envs_per_worker * config.rollout_fragment_length,
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
