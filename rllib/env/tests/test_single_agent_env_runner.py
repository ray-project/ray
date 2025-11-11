import unittest
from functools import partial
from unittest.mock import patch

import gymnasium as gym

import ray
from ray import tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.env_runner import StepFailedRecreateEnvError
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.examples.envs.classes.simple_corridor import SimpleCorridor
from ray.rllib.utils.test_utils import check


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

    def test_distributed_env_runner(self):
        """Tests, whether SingleAgentEnvRunner can be distributed."""

        remote_class = ray.remote(num_cpus=1, num_gpus=0)(SingleAgentEnvRunner)

        # Test with both parallelized sub-envs and w/o.
        async_vectorization_mode = [False, True]

        for async_ in async_vectorization_mode:

            for env_spec in ["tune-registered", "CartPole-v1", SimpleCorridor]:
                config = (
                    AlgorithmConfig().environment(env_spec)
                    # Vectorize x5 and by default, rollout 10 timesteps per individual
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
                    self.assertIn(
                        sum(len(e) for e in episodes),
                        [
                            config.num_envs_per_env_runner
                            * config.rollout_fragment_length
                            + i
                            for i in range(config.num_envs_per_env_runner)
                        ],
                    )

    def test_sample(self):
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_envs_per_env_runner=2,
                rollout_fragment_length=64,
            )
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Expect error if both num_timesteps and num_episodes given.
        self.assertRaises(
            AssertionError,
            lambda: env_runner.sample(
                num_timesteps=10, num_episodes=10, random_actions=True
            ),
        )

        # Sample 10 episodes (5 per env, because num_envs_per_env_runner=2)
        # Repeat 100 times
        for _ in range(100):
            episodes = env_runner.sample(num_episodes=10, random_actions=True)
            check(len(episodes), 10)
            # Since we sampled complete episodes, there should be no ongoing episodes
            # being returned.
            self.assertTrue(all(e.is_done for e in episodes))

        # Sample 10 timesteps (5 per env)
        # Repeat 100 times
        for _ in range(100):
            episodes = env_runner.sample(num_timesteps=10, random_actions=True)
            # Check the sum of lengths of all episodes returned.
            sum_ = sum(map(len, episodes))
            self.assertTrue(sum_ in [10, 11])

        # Sample rollout_fragment_length=64, 100 times
        # Repeat 100 times
        for _ in range(100):
            episodes = env_runner.sample(random_actions=True)
            # Check, whether the sum of lengths of all episodes returned is 128
            # 2 (num_env_per_worker) * 64 (rollout_fragment_length).
            sum_ = sum(map(len, episodes))
            self.assertTrue(sum_ in [128, 129])

    @patch(target="ray.rllib.env.env_runner.logger")
    def test_step_failed_reset_required(self, mock_logger):
        """Tests, whether SingleAgentEnvRunner can handle StepFailedResetRequired."""

        # Define an env that raises StepFailedResetRequired
        class ErrorRaisingEnv(gym.Env):
            def __init__(self, config=None):
                # As per gymnasium standard, provide observation and action spaces in your
                # constructor.
                self.observation_space = gym.spaces.Discrete(2)
                self.action_space = gym.spaces.Discrete(2)
                self.exception_type = config["exception_type"]

            def reset(self, *, seed=None, options=None):
                return self.observation_space.sample(), {}

            def step(self, action):
                raise self.exception_type()

        config = (
            AlgorithmConfig()
            .environment(
                ErrorRaisingEnv,
                env_config={"exception_type": StepFailedRecreateEnvError},
            )
            .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            .fault_tolerance(restart_failed_sub_environments=True)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Check that we don't log the error on the first step (because we don't raise StepFailedResetRequired)
        # We need two steps because the first one naturally raises ResetNeeded because we try to step before the env is reset.
        env_runner._try_env_reset()
        env_runner._try_env_step(actions=[None])

        assert mock_logger.exception.call_count == 0

        config.environment(ErrorRaisingEnv, env_config={"exception_type": ValueError})

        env_runner = SingleAgentEnvRunner(config=config)

        # Check that we don't log the error on the first step (because we don't raise StepFailedResetRequired)
        # We need two steps because the first one naturally raises ResetNeeded because we try to step before the env is reset.
        env_runner._try_env_reset()
        env_runner._try_env_step(actions=[None])

        assert mock_logger.exception.call_count == 1

    def test_vector_env(self):
        """Tests, whether SingleAgentEnvRunner can run various vectorized envs."""

        for env in ["CartPole-v1", SimpleCorridor, "tune-registered"]:
            config = (
                AlgorithmConfig()
                .environment(env)
                .env_runners(
                    num_envs_per_env_runner=5,
                    rollout_fragment_length=10,
                )
            )

            env_runner = SingleAgentEnvRunner(config=config)

            # Sample with the async-vectorized env.
            episodes = env_runner.sample(random_actions=True)
            self.assertEqual(
                sum(len(e) for e in episodes),
                config.num_envs_per_env_runner * config.rollout_fragment_length,
            )
            env_runner.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
