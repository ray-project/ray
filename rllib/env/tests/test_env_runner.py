"""Shared tests for SingleAgentEnvRunner and MultiAgentEnvRunner.

These tests are parameterized to run against both runner types, ensuring
consistent behavior across the EnvRunner interface.
Additionally, the tests are split into separate classes for different testing
components.
"""
import pytest
from conftest import CallbackTracker, ConfigurableFailingEnv, get_t_started, ConnectorTracker

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole


class TestEnvRunnerSampling:
    """Tests for sampling functionality common to both runner types."""

    repeats = 10

    def test_sample_num_episodes(self, env_runner, num_episodes=3):
        """Test sampling a specific number of episodes."""
        for _ in range(self.repeats):
            episodes = env_runner.sample(num_episodes=num_episodes, random_actions=True)
            assert len(episodes) == num_episodes
            assert all(e.is_done for e in episodes)

    def test_sample_num_timesteps(self, env_runner, num_timesteps=20):
        """Test sampling a specific number of timesteps."""
        for _ in range(self.repeats):
            episodes = env_runner.sample(
                num_timesteps=num_timesteps, random_actions=True
            )
            total_timesteps = sum(len(e) for e in episodes)
            # Allow some slack for vectorized envs (up to num_envs extra)
            assert (
                num_timesteps <= total_timesteps <= num_timesteps + env_runner.num_envs
            )

    def test_sample_default_rollout_fragment(self, env_runner, env_runner_config):
        """Test sampling with default rollout_fragment_length."""
        for _ in range(self.repeats):
            episodes = env_runner.sample(random_actions=True)
            total_timesteps = sum(len(e) for e in episodes)
            # Should be approximately rollout_fragment_length * num_envs
            expected_min = env_runner_config.rollout_fragment_length
            assert expected_min <= total_timesteps

    def test_sample_validation_both_args_error(self, env_runner):
        """Test that providing both num_timesteps and num_episodes raises error."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_timesteps=10, num_episodes=10, random_actions=True)

    def test_sample_validation_negative_timesteps(self, env_runner, runner_type):
        """Test that negative num_timesteps raises error (SingleAgent only)."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_timesteps=-1, random_actions=True)

    def test_sample_validation_negative_episodes(self, env_runner, runner_type):
        """Test that negative num_episodes raises error (SingleAgent only)."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_episodes=-1, random_actions=True)

    def test_zero_timesteps_sample(self, env_runner):
        """Test sampling with zero timesteps."""
        # This might either return empty or raise - document the behavior
        episodes = env_runner.sample(num_timesteps=0, random_actions=True)
        # If it doesn't raise, should return empty or minimal
        assert isinstance(episodes, list)
        assert len(episodes) == 0

    def test_sample_force_reset_with_timesteps(self, env_runner, runner_type):
        """Test that force_reset starts fresh episodes when using num_timesteps."""
        # Sample partial episode
        env_runner.sample(num_timesteps=5, random_actions=True)
        # Sample with force_reset
        episodes = env_runner.sample(
            num_timesteps=10, random_actions=True, force_reset=True
        )

        assert all(get_t_started(e, runner_type) for e in episodes)

    def test_sample_force_reset_with_episodes(self, env_runner, runner_type):
        """Test that force_reset works with num_episodes."""
        # Sample some episodes first
        env_runner.sample(num_episodes=1, random_actions=True)
        # Sample with force_reset (should still work fine)
        episodes = env_runner.sample(
            num_episodes=2, random_actions=True, force_reset=True
        )
        assert len(episodes) == 2
        assert all(get_t_started(e, runner_type) for e in episodes)


class TestEnvRunnerEpisodeContinuation:
    """Tests for episode continuation across sample() calls."""

    def test_episode_continuation_between_samples(self, env_runner, runner_type):
        """Test that episodes continue correctly across sample() calls."""
        # Sample partial episode (fewer timesteps than episode length)
        episode_1 = env_runner.sample(num_timesteps=5, random_actions=True)
        episode_1_ids = {e.id_ for e in episode_1 if not e.is_done}
        assert len(episode_1_ids) > 0

        # Sample more timesteps - should continue the same episodes
        episodes_2 = env_runner.sample(num_timesteps=5, random_actions=True)
        continued_ids = {e.id_ for e in episodes_2 if get_t_started(e, runner_type) > 0}

        # The continued episodes should have IDs from the first batch
        if continued_ids:
            assert continued_ids.issubset(episode_1_ids)

    def test_force_reset_breaks_continuation(self, env_runner, runner_type):
        """Test that force_reset prevents episode continuation."""
        # Sample partial episode
        episodes_1 = env_runner.sample(num_timesteps=5, random_actions=True)

        # Sample with force_reset - should NOT continue
        episodes_2 = env_runner.sample(
            num_timesteps=5, random_actions=True, force_reset=True
        )

        # All episodes should start fresh
        assert all(get_t_started(e, runner_type) == 0 for e in episodes_2)

        # check there is no overlap in episode ids
        episode_1_ids = {e.id_ for e in episodes_1}
        episode_2_ids = {e.id_ for e in episodes_2}
        assert len(episode_1_ids.intersection(episode_2_ids)) == 0

    def test_complete_episodes_dont_continue(self, env_runner, runner_type):
        """Test that completed episodes are not continued."""
        episodes_1 = env_runner.sample(num_episodes=2, random_actions=True)
        assert all(e.is_done for e in episodes_1)

        # Sample more complete episodes
        episodes_2 = env_runner.sample(num_episodes=2, random_actions=True)

        # All episodes should start fresh
        assert all(get_t_started(e, runner_type) == 0 for e in episodes_2)
        # check there is no overlap in episode ids
        episode_1_ids = {e.id_ for e in episodes_1}
        episode_2_ids = {e.id_ for e in episodes_2}
        assert len(episode_1_ids.intersection(episode_2_ids)) == 0


class TestEnvRunnerStateManagement:
    """Tests for state management common to both runner types.

    Note: SingleAgentEnvRunner with AlgorithmConfig (not PPOConfig) doesn't create
    a module by default, so get_state() fails. These tests use PPOConfig for both
    runner types to ensure modules are created.
    """

    def test_get_state_returns_dict(self, runner_type, env_runner_cls, ray_init):
        """Test that get_state returns a dictionary."""
        # Use PPOConfig for both to ensure module is created
        if runner_type == "single_agent":
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            )
            runner = SingleAgentEnvRunner(config=config)
        elif runner_type == "multi_agent":
            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            )
            runner = MultiAgentEnvRunner(config=config)
        else:
            raise ValueError(f"Unknown runner type: {runner_type}")

        try:
            state = runner.get_state()
            assert isinstance(state, dict)
            assert "rl_module" in runner.get_state()

            runner.sample(num_episodes=1, random_actions=True)

            # recheck after sample
            state = runner.get_state()
            assert isinstance(state, dict)
            assert "rl_module" in runner.get_state()

            # check that a new env runner can be updated based on an older state
            new_runner = env_runner_cls(config=config)
            try:
                new_runner.set_state(state)

                # roundtrip the runner state
                new_state = new_runner.get_state()
                assert set(state.keys()) == set(new_state.keys())
            finally:
                new_runner.stop()
        finally:
            runner.stop()


class TestEnvRunnerMetrics:
    """Tests for metrics collection common to both runner types."""

    # TOOD after https://github.com/ray-project/ray/pull/56838 merged


class TestEnvRunnerErrorHandling:
    """Tests for error handling common to both runner types."""

    def test_env_step_failure_with_restart_enabled(self, runner_type, ray_init):
        """Test recovery from environment step failures when restart is enabled."""
        ConfigurableFailingEnv.reset_counters()

        if runner_type == "single_agent":
            config = (
                AlgorithmConfig()
                .environment(
                    ConfigurableFailingEnv,
                    env_config={"fail_on_step": True, "fail_after_n_steps": 5},
                )
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
                .fault_tolerance(restart_failed_sub_environments=True)
            )
            runner = SingleAgentEnvRunner(config=config)
        else:
            # For multi-agent, we need a multi-agent failing env
            # Skip this test for multi-agent as it requires different setup
            pytest.skip("Multi-agent failing env test requires different setup")

        # TODO: Reimplement
        try:
            # This should handle the failure gracefully
            # The env will fail after 5 steps and be recreated
            runner._try_env_reset()
            for _ in range(3):
                result = runner._try_env_step(actions=[0])
                if result == "env_step_failure":
                    # Failure was handled, env should be recreated
                    break
        finally:
            runner.stop()
            ConfigurableFailingEnv.reset_counters()


class TestEnvRunnerCallbacks:
    """Tests for callback invocations common to both runner types."""

    def test_on_episode_end_called(self, runner_type, ray_init):
        """Test that on_episode_end callback is called for completed episodes."""
        CallbackTracker.reset()

        if runner_type == "single_agent":
            config = (
                AlgorithmConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1)
                .callbacks(CallbackTracker)
            )
            runner = SingleAgentEnvRunner(config=config)
        else:
            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                .env_runners(num_envs_per_env_runner=1)
                .callbacks(CallbackTracker)
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            # Sample complete episodes
            runner.sample(num_episodes=2, random_actions=True)

            # Check callbacks were called
            end_calls = CallbackTracker.get_calls("on_episode_end")
            assert (
                len(end_calls) == 2
            ), f"Expected at least 2 on_episode_end calls, got {len(end_calls)}"
        finally:
            runner.stop()
            CallbackTracker.reset()

    def test_on_episode_step_called(self, runner_type, ray_init):
        """Test that on_episode_step callback is called for each step."""
        CallbackTracker.reset()

        if runner_type == "single_agent":
            config = (
                AlgorithmConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
                .callbacks(CallbackTracker)
            )
            runner = SingleAgentEnvRunner(config=config)
        else:
            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
                .callbacks(CallbackTracker)
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            runner.sample(num_timesteps=10, random_actions=True)

            step_calls = CallbackTracker.get_calls("on_episode_step")
            # Should have approximately 10 step calls (may vary slightly)
            assert (
                len(step_calls) >= 5
            ), f"Expected step callbacks, got {len(step_calls)}"
        finally:
            runner.stop()
            CallbackTracker.reset()

    def test_on_sample_end_called(self, runner_type, ray_init):
        """Test that on_sample_end callback is called after each sample()."""
        CallbackTracker.reset()

        if runner_type == "single_agent":
            config = (
                AlgorithmConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1)
                .callbacks(CallbackTracker)
            )
            runner = SingleAgentEnvRunner(config=config)
        else:
            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                .env_runners(num_envs_per_env_runner=1)
                .callbacks(CallbackTracker)
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            # Sample twice
            runner.sample(num_timesteps=5, random_actions=True)
            runner.sample(num_timesteps=5, random_actions=True)

            sample_end_calls = CallbackTracker.get_calls("on_sample_end")
            assert (
                len(sample_end_calls) == 2
            ), f"Expected 2 on_sample_end calls, got {len(sample_end_calls)}"
        finally:
            runner.stop()
            CallbackTracker.reset()


class TestEnvRunnerConnectors:

    def test_on_episode_end_callback(self):
        """Check that callback only happens once for each completed episode.

        Related to https://github.com/ray-project/ray/issues/55452
        """
        config = (
            PPOConfig()
            .environment(
                MultiAgentCartPole,
                env_config={"num_agents": 1},
            )
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=(lambda aid, *args, **kwargs: "p0")
            )
            .env_runners(
                env_to_module_connector=ConnectorTracker,
            )
        )

        for num_envs, num_episodes in [(1, 1), (4, 4), (1, 4)]:
            config.env_runners(num_envs_per_env_runner=num_envs)

            env_runner = MultiAgentEnvRunner(config=config)

            self.assertTrue(
                isinstance(env_runner._env_to_module.connectors[0],
                           ConnectorTracker)
            )
            self.assertEqual(
                env_runner._env_to_module.connectors[0].episode_end_counter, 0
            )

            sampled_episodes = env_runner.sample(
                num_episodes=num_episodes, random_actions=True
            )
            self.assertEqual(len(sampled_episodes), num_episodes)
            self.assertEqual(
                env_runner._env_to_module.connectors[0].episode_end_counter,
                num_episodes,
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
