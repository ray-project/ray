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

        # Sample 10 episodes 10 times.
        for _ in range(10):
            done_episodes, ongoing_episodes = env_runner.sample(
                num_episodes=10, random_actions=True, with_render_data=True
            )
            # Since we sampled complete episodes, there should be no ongoing episodes
            # being returned.
            assert len(ongoing_episodes) == 0
            # Check, whether all done_episodes returned are indeed terminated.
            for eps in done_episodes:
                assert eps.is_terminated

        # Sample (by default setting: rollout_fragment_length=64) 10 times.
        for _ in range(10):
            done_episodes, ongoing_episodes = env_runner.sample(random_actions=True)
            for eps in done_episodes:
                assert eps.is_terminated
            for eps in ongoing_episodes:
                assert not eps.is_terminated

    def test_distributed_simple_env_runner(self):
        """Tests, whether SimpleEnvRunner can be distributed."""
        config = (
            AlgorithmConfig().environment("CartPole-v1")
            # Vectorize x2 and by default, rollout 64 timesteps per individual env.
            .rollouts(num_envs_per_worker=4, rollout_fragment_length=10)
        )

        remote_class = SingleAgentGymEnvRunner.as_remote(num_cpus=1, num_gpus=0)
        array = [remote_class.remote(config=config) for _ in range(5)]
        # Sample in parallel.
        results = [a.sample.remote() for a in array]
        results = ray.get(results)
        print(results)
