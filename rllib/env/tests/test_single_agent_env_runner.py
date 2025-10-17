import unittest
from unittest.mock import patch

import gymnasium as gym
from gymnasium.vector import SyncVectorEnv, VectorEnv
from gymnasium.envs.classic_control.cartpole import CartPoleVectorEnv, CartPoleEnv

import ray
from ray import tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.env_runner import StepFailedRecreateEnvError
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.simple_corridor import SimpleCorridor


class TestSingleAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

        tune.register_env(
            "tune-registered",
            lambda **cfg: CartPoleEnv(**cfg),
        )
        tune.register_env(
            "tune-registered-vec",
            lambda num_envs, **cfg: CartPoleVectorEnv(num_envs=num_envs, **cfg),
        )

        gym.register(
            "TestEnv-v0",
            entry_point=CartPoleEnv,
        )
        gym.register(
            "TestVecEnv-v0",
            vector_entry_point=CartPoleVectorEnv,
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

    def test_sample(
        self,
        num_envs_per_env_runner=5,
        expected_episodes=10,
        expected_timesteps=20,
        rollout_fragment_length=64,
    ):
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_envs_per_env_runner=num_envs_per_env_runner,
                rollout_fragment_length=rollout_fragment_length,
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

        # Sample 10 episodes (2 per env, because num_envs_per_env_runner=5)
        # Repeat 100 times
        for _ in range(100):
            episodes = env_runner.sample(
                num_episodes=expected_episodes, random_actions=True
            )
            self.assertTrue(len(episodes) == expected_episodes)
            # Since we sampled complete episodes, there should be no ongoing episodes
            # being returned.
            self.assertTrue(all(e.is_done for e in episodes))
            self.assertTrue(all(e.t_started == 0 for e in episodes))

        # Sample 20 timesteps (4 per env)
        # Repeat 100 times
        env_runner.sample(random_actions=True)  # for the `e.t_started > 0`
        for _ in range(100):
            episodes = env_runner.sample(
                num_timesteps=expected_timesteps, random_actions=True
            )
            # Check the sum of lengths of all episodes returned.
            total_timesteps = sum(len(e) for e in episodes)
            self.assertTrue(
                expected_timesteps
                <= total_timesteps
                <= expected_timesteps + num_envs_per_env_runner
            )
            self.assertTrue(any(e.t_started > 0 for e in episodes))

        # Sample a number of timesteps thats not a factor of the number of environments
        # Repeat 100 times
        expected_uneven_timesteps = expected_timesteps + num_envs_per_env_runner // 2
        for _ in range(100):
            episodes = env_runner.sample(
                num_timesteps=expected_uneven_timesteps, random_actions=True
            )
            # Check the sum of lengths of all episodes returned.
            total_timesteps = sum(len(e) for e in episodes)
            self.assertTrue(
                expected_uneven_timesteps
                <= total_timesteps
                <= expected_uneven_timesteps + num_envs_per_env_runner,
            )
            self.assertTrue(any(e.t_started > 0 for e in episodes))

        # Sample rollout_fragment_length=64, 100 times
        # Repeat 100 times
        for _ in range(100):
            episodes = env_runner.sample(random_actions=True)
            # Check, whether the sum of lengths of all episodes returned is 320
            # 5 (num_env_per_worker) * 64 (rollout_fragment_length).
            total_timesteps = sum(len(e) for e in episodes)
            self.assertTrue(
                num_envs_per_env_runner * rollout_fragment_length
                <= total_timesteps
                <= (num_envs_per_env_runner + 1) * rollout_fragment_length
            )
            self.assertTrue(any(e.t_started > 0 for e in episodes))

        # Test that force_reset will create episodes from scratch even with `num_timesteps`
        episodes = env_runner.sample(
            num_timesteps=expected_timesteps, random_actions=True, force_reset=True
        )
        self.assertTrue(any(e.t_started == 0 for e in episodes))
        episodes = env_runner.sample(
            num_timesteps=expected_timesteps, random_actions=True, force_reset=False
        )
        self.assertTrue(any(e.t_started > 0 for e in episodes))

    @patch(target="ray.rllib.env.env_runner.logger")
    def test_step_failed_reset_required(self, mock_logger):
        """Tests, whether SingleAgentEnvRunner can handle StepFailedResetRequired."""

        # Define an env that raises StepFailedResetRequired
        class ErrorRaisingEnv(gym.Env):
            def __init__(self, exception_type):
                # As per gymnasium standard, provide observation and action spaces in your
                # constructor.
                self.observation_space = gym.spaces.Discrete(2)
                self.action_space = gym.spaces.Discrete(2)
                self.exception_type = exception_type

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

    def test_env_vectorizer(self):
        """Tests, whether SingleAgentEnvRunner can run various vectorized envs."""

        for env in [
            "CartPole-v1",
            CartPoleEnv,
            "tune-registered",
            "TestEnv-v0",
            "ale_py:ALE/Pong-v5",
        ]:
            config = (
                AlgorithmConfig()
                .environment(env)
                .env_runners(
                    num_envs_per_env_runner=5,
                    rollout_fragment_length=10,
                )
            )

            env_runner = SingleAgentEnvRunner(config=config)
            assert isinstance(env_runner.env.env, SyncVectorEnv)

            # Sample with the async-vectorized env.
            episodes = env_runner.sample(random_actions=True)
            self.assertTrue(
                config.num_envs_per_env_runner * config.rollout_fragment_length
                <= sum(len(e) for e in episodes)
                < config.num_envs_per_env_runner * (config.rollout_fragment_length + 1)
            )
            env_runner.stop()

    def test_self_vectorized_env(self):
        """Tests, whether SingleAgentEnvRunner can run various vectorized envs."""

        for env in [
            "CartPole-v1",
            CartPoleVectorEnv,
            "tune-registered-vec",
            "TestVecEnv-v0",
            "ale_py:ALE/Pong-v5",
        ]:
            config = (
                AlgorithmConfig()
                .environment(env)
                .env_runners(
                    num_envs_per_env_runner=5,
                    rollout_fragment_length=10,
                    gym_env_vectorize_mode=gym.VectorizeMode.VECTOR_ENTRY_POINT,
                )
            )

            env_runner = SingleAgentEnvRunner(config=config)
            assert isinstance(env_runner.env.env, VectorEnv) and not isinstance(
                env_runner.env.env, SyncVectorEnv
            )

            # Sample with the async-vectorized env.
            episodes = env_runner.sample(random_actions=True)
            self.assertTrue(
                config.num_envs_per_env_runner * config.rollout_fragment_length
                <= sum(len(e) for e in episodes)
                < config.num_envs_per_env_runner * (config.rollout_fragment_length + 1)
            )
            env_runner.stop()

    def test_env_context(self):
        """Tests, whether SingleAgentEnvRunner can pass kwargs to the environments correctly."""

        # Test gymnasium.Env
        for env in ["CartPole-v1", CartPoleEnv, "tune-registered", "TestEnv-v0"]:
            config = AlgorithmConfig().environment(
                env, env_config={"sutton_barto_reward": True}
            )
            env_runner = SingleAgentEnvRunner(config=config)
            assert env_runner.env.env.get_attr("_sutton_barto_reward") == (True,)

        # Test gymnasium.vector.VectorEnv
        for env in [
            "CartPole-v1",
            CartPoleVectorEnv,
            "tune-registered-vec",
            "TestVecEnv-v0",
        ]:
            config = (
                AlgorithmConfig()
                .environment(env, env_config={"sutton_barto_reward": True})
                .env_runners(
                    gym_env_vectorize_mode=gym.VectorizeMode.VECTOR_ENTRY_POINT
                )
            )
            env_runner = SingleAgentEnvRunner(config=config)

            assert env_runner.env.env._sutton_barto_reward is True

        # Test registered environment and vector env with pre-set kwargs
        gym.register(
            "TestEnv-v1",
            entry_point=CartPoleEnv,
            vector_entry_point=CartPoleVectorEnv,
            kwargs={"sutton_barto_reward": True},
        )
        config = AlgorithmConfig().environment("TestEnv-v1")
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("_sutton_barto_reward") == (True,)

        config = (
            AlgorithmConfig()
            .environment("TestEnv-v1")
            .env_runners(gym_env_vectorize_mode="vector_entry_point")
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env._sutton_barto_reward is True


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
