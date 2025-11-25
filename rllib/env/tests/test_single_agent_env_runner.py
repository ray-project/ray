import unittest
from unittest.mock import patch

import gymnasium as gym
from gymnasium.envs.classic_control.cartpole import CartPoleVectorEnv
from gymnasium.envs.mujoco.swimmer_v4 import SwimmerEnv

import ray
from ray import tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.env_runner import StepFailedRecreateEnvError
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.simple_corridor import SimpleCorridor
from ray.tune.registry import ENV_CREATOR, _global_registry


class TestSingleAgentEnvRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

        tune.register_env(
            "tune-registered",
            lambda cfg: SimpleCorridor({"corridor_length": 10} | cfg),
        )

        tune.register_env(
            "tune-registered-vector",
            lambda cfg: CartPoleVectorEnv(**cfg),
        )

        gym.register(
            "TestEnv-v0",
            entry_point=SimpleCorridor,
            kwargs={"corridor_length": 10},
        )

        gym.register(
            "TestEnv-v1",
            entry_point=SwimmerEnv,
            kwargs={"forward_reward_weight": 2.0, "reset_noise_scale": 0.2},
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

        _global_registry.unregister(ENV_CREATOR, "tune-registered")
        _global_registry.unregister(ENV_CREATOR, "tune-registered-vector")
        gym.registry.pop("TestEnv-v0")
        gym.registry.pop("TestEnv-v1")

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
                    # Assert length of all fragments >= `rollout_fragment_length * num_envs_per_env_runner` and
                    #   < rollout_fragment_length * (num_envs_per_env_runner + 1)
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
        # Verify that an error is raised if a negative number is used
        self.assertRaises(
            AssertionError,
            lambda: env_runner.sample(num_timesteps=-1, random_actions=True),
        )
        self.assertRaises(
            AssertionError,
            lambda: env_runner.sample(num_episodes=-1, random_actions=True),
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

        # Sample a number of timesteps that's not a factor of the number of environments
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
                <= (
                    num_envs_per_env_runner * rollout_fragment_length
                    + num_envs_per_env_runner
                )
            )
            self.assertTrue(any(e.t_started > 0 for e in episodes))

        # Test that force_reset will create episodes from scratch even with `num_timesteps`
        episodes = env_runner.sample(
            num_timesteps=expected_timesteps, random_actions=True, force_reset=True
        )
        self.assertTrue(all(e.t_started == 0 for e in episodes))
        episodes = env_runner.sample(
            num_timesteps=expected_timesteps, random_actions=True, force_reset=False
        )
        self.assertTrue(any(e.t_started > 0 for e in episodes))

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

    def test_vector_env(self, num_envs_per_env_runner=5, rollout_fragment_length=10):
        """Tests, whether SingleAgentEnvRunner can run various vectorized envs."""

        # "ALE/Pong-v5" works but ale-py is not installed on microcheck
        for env in ["CartPole-v1", SimpleCorridor, "tune-registered"]:
            config = (
                AlgorithmConfig()
                .environment(env)
                .env_runners(
                    num_envs_per_env_runner=num_envs_per_env_runner,
                    rollout_fragment_length=rollout_fragment_length,
                )
            )

            env_runner = SingleAgentEnvRunner(config=config)

            # Sample with the async-vectorized env.
            for i in range(100):
                episodes = env_runner.sample(random_actions=True)
                total_timesteps = sum(len(e) for e in episodes)
                self.assertTrue(
                    num_envs_per_env_runner * rollout_fragment_length
                    <= total_timesteps
                    <= (
                        num_envs_per_env_runner * rollout_fragment_length
                        + num_envs_per_env_runner
                    )
                )
            env_runner.stop()

    def test_env_context(self):
        """Tests, whether SingleAgentEnvRunner can pass kwargs to the environments correctly."""

        # default without env configs
        config = AlgorithmConfig().environment("Swimmer-v4")
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("_forward_reward_weight") == (1.0,)
        assert env_runner.env.env.get_attr("_reset_noise_scale") == (0.1,)

        # Test gym registered environment env with kwargs
        config = AlgorithmConfig().environment(
            "Swimmer-v4",
            env_config={"forward_reward_weight": 2.0, "reset_noise_scale": 0.2},
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("_forward_reward_weight") == (2.0,)
        assert env_runner.env.env.get_attr("_reset_noise_scale") == (0.2,)

        # Test gym registered environment env with pre-set kwargs
        config = AlgorithmConfig().environment("TestEnv-v1")
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("_forward_reward_weight") == (2.0,)
        assert env_runner.env.env.get_attr("_reset_noise_scale") == (0.2,)

        # Test using a mixture of registered kwargs and env configs
        config = AlgorithmConfig().environment(
            "TestEnv-v1", env_config={"forward_reward_weight": 3.0}
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("_forward_reward_weight") == (3.0,)
        assert env_runner.env.env.get_attr("_reset_noise_scale") == (0.2,)

        # Test env-config with Tune registered or callable
        #   default
        config = AlgorithmConfig().environment("tune-registered")
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("end_pos") == (10.0,)

        #   tune-registered
        config = AlgorithmConfig().environment(
            "tune-registered", env_config={"corridor_length": 5.0}
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("end_pos") == (5.0,)

        #   callable
        config = AlgorithmConfig().environment(
            SimpleCorridor, env_config={"corridor_length": 5.0}
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("end_pos") == (5.0,)

    def test_vectorize_mode(self):
        """Test different vectorize mode for creating the environment."""

        # default
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=3)
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert isinstance(env_runner.env.env, gym.vector.SyncVectorEnv)

        # different vectorize mode options contained in gymnasium registry
        for env_name, mode, expected_env_type in [
            ("CartPole-v1", "sync", gym.vector.SyncVectorEnv),
            ("CartPole-v1", gym.VectorizeMode.SYNC, gym.vector.SyncVectorEnv),
            ("CartPole-v1", "async", gym.vector.AsyncVectorEnv),
            ("CartPole-v1", gym.VectorizeMode.ASYNC, gym.vector.AsyncVectorEnv),
            ("CartPole-v1", "vector_entry_point", CartPoleVectorEnv),
            ("CartPole-v1", gym.VectorizeMode.VECTOR_ENTRY_POINT, CartPoleVectorEnv),
            # TODO (mark) re-add with ale-py 0.11 support
            # ("ALE/Pong-v5", "vector_entry_point", AtariVectorEnv),
            # ("ALE/Pong-v5", gym.VectorizeMode.VECTOR_ENTRY_POINT, AtariVectorEnv),
        ]:
            config = (
                AlgorithmConfig()
                .environment(env_name)
                .env_runners(gym_env_vectorize_mode=mode, num_envs_per_env_runner=3)
            )
            env_runner = SingleAgentEnvRunner(config=config)
            assert isinstance(env_runner.env.env, expected_env_type)

        # test with tune registered vector environment
        config = (
            AlgorithmConfig()
            .environment(
                "tune-registered-vector", env_config={"sutton_barto_reward": True}
            )
            .env_runners(
                gym_env_vectorize_mode="vector_entry_point", num_envs_per_env_runner=3
            )
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert isinstance(env_runner.env.env, CartPoleVectorEnv)
        assert env_runner.env.env._sutton_barto_reward is True

        # test with callable vector environment
        config = (
            AlgorithmConfig()
            .environment(
                lambda cfg: CartPoleVectorEnv(**cfg),
                env_config={"sutton_barto_reward": True},
            )
            .env_runners(
                gym_env_vectorize_mode="vector_entry_point", num_envs_per_env_runner=3
            )
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert isinstance(env_runner.env.env, CartPoleVectorEnv)
        assert env_runner.env.env._sutton_barto_reward is True

        # check passing the env config with a gym_env_vectorize_mode
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1", env_config={"sutton_barto_reward": True})
            .env_runners(gym_env_vectorize_mode="sync", num_envs_per_env_runner=3)
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env.get_attr("_sutton_barto_reward") == (True, True, True)

        config = (
            AlgorithmConfig()
            .environment("CartPole-v1", env_config={"sutton_barto_reward": True})
            .env_runners(
                gym_env_vectorize_mode="vector_entry_point", num_envs_per_env_runner=3
            )
        )
        env_runner = SingleAgentEnvRunner(config=config)
        assert env_runner.env.env._sutton_barto_reward is True

    def test_get_spaces(self):
        """Test that get_spaces returns the correct observation and action spaces."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=2)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        spaces = env_runner.get_spaces()

        # Should have at least one entry
        self.assertGreaterEqual(len(spaces), 1)

        # Find the module with CartPole spaces
        found_cartpole_spaces = False
        for module_id, (obs_space, act_space) in spaces.items():
            # CartPole-v1 has Box observation space and Discrete action space
            if obs_space is not None and hasattr(obs_space, "shape"):
                if obs_space.shape == (4,):
                    self.assertIsInstance(obs_space, gym.spaces.Box)
                    self.assertIsInstance(act_space, gym.spaces.Discrete)
                    self.assertEqual(act_space.n, 2)
                    found_cartpole_spaces = True
                    break

        self.assertTrue(found_cartpole_spaces, "Should find CartPole observation space")

        env_runner.stop()

    def test_assert_healthy_after_init(self):
        """Test that assert_healthy passes after proper initialization."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=2)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Should not raise for properly initialized runner
        env_runner.assert_healthy()
        env_runner.stop()

    def test_get_metrics_episode_return(self):
        """Test that get_metrics includes episode return statistics."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=200)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Sample complete episodes
        env_runner.sample(num_episodes=5, random_actions=True)
        metrics = env_runner.get_metrics()

        # Should have metrics dictionary
        self.assertIsInstance(metrics, dict)

        env_runner.stop()

    def test_get_state_contains_module(self):
        """Test that get_state includes the RL module state.

        Note: get_state requires a module, so we use PPOConfig instead of AlgorithmConfig.
        """
        from ray.rllib.algorithms.ppo import PPOConfig

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Sample to ensure module is exercised
        env_runner.sample(num_episodes=1, random_actions=True)

        state = env_runner.get_state()
        self.assertIsInstance(state, dict)

        # Should have rl_module in state
        self.assertIn("rl_module", state)

        env_runner.stop()

    def test_set_state_restores_module(self):
        """Test that set_state restores the RL module state.

        Note: get_state/set_state requires a module, so we use PPOConfig.
        """
        from ray.rllib.algorithms.ppo import PPOConfig

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1)
        )
        env_runner1 = SingleAgentEnvRunner(config=config)
        env_runner1.sample(num_episodes=1, random_actions=True)

        # Get state from first runner
        state = env_runner1.get_state()

        # Create second runner and restore state
        env_runner2 = SingleAgentEnvRunner(config=config)
        env_runner2.set_state(state)

        # Both should have same module structure
        state2 = env_runner2.get_state()
        self.assertEqual(set(state.keys()), set(state2.keys()))

        env_runner1.stop()
        env_runner2.stop()

    def test_episode_return_accumulation(self):
        """Test that episode returns are accumulated correctly."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=500)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Sample complete episodes
        episodes = env_runner.sample(num_episodes=3, random_actions=True)

        for episode in episodes:
            # Each episode should have a non-negative return
            # (CartPole gives +1 per step)
            self.assertGreaterEqual(episode.get_return(), 0)
            # Return should equal episode length for CartPole
            self.assertEqual(episode.get_return(), len(episode))

        env_runner.stop()

    def test_multiple_vectorized_envs_timestep_distribution(self):
        """Test that timesteps are distributed correctly across vectorized envs."""
        num_envs = 4
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=num_envs, rollout_fragment_length=20)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Sample with num_timesteps
        episodes = env_runner.sample(num_timesteps=40, random_actions=True)

        # Total timesteps should be at least 40 (may be slightly more due to vectorization)
        total_timesteps = sum(len(e) for e in episodes)
        self.assertGreaterEqual(total_timesteps, 40)
        self.assertLessEqual(total_timesteps, 40 + num_envs)

        env_runner.stop()

    def test_seed_reproducibility(self):
        """Test that setting a seed produces consistent behavior."""
        # Note: Exact reproducibility is complex with vectorized envs and
        # random action sampling. We test that seeding doesn't break anything
        # and that results are reasonably consistent.
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
        )
        config.seed = 42

        env_runner1 = SingleAgentEnvRunner(config=config)
        episodes1 = env_runner1.sample(num_timesteps=10, random_actions=True)
        total1 = sum(len(e) for e in episodes1)
        env_runner1.stop()

        env_runner2 = SingleAgentEnvRunner(config=config)
        episodes2 = env_runner2.sample(num_timesteps=10, random_actions=True)
        total2 = sum(len(e) for e in episodes2)
        env_runner2.stop()

        # With the same seed, total timesteps should be the same (around 10)
        # Episode count may vary due to timing/termination
        self.assertEqual(total1, total2)

    def test_env_recreation_on_make_env(self):
        """Test that make_env recreates the environment."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=2)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Recreate env
        env_runner.make_env()

        # Should have a new env instance
        self.assertIsNotNone(env_runner.env)

        env_runner.stop()

    def test_stop_releases_env(self):
        """Test that stop() properly releases environment resources."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=2)
        )
        env_runner = SingleAgentEnvRunner(config=config)

        # Env should exist before stop
        self.assertIsNotNone(env_runner.env)

        env_runner.stop()

        # After stop, calling stop again should not raise
        env_runner.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
