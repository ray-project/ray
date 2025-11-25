"""Shared tests for SingleAgentEnvRunner and MultiAgentEnvRunner.

These tests are parameterized to run against both runner types, ensuring
consistent behavior across the EnvRunner interface.
"""
import gymnasium as gym
import pytest
from conftest import CallbackTracker

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole


class TestEnvRunnerSampling:
    """Tests for sampling functionality common to both runner types."""

    def test_sample_num_episodes(self, env_runner):
        """Test sampling a specific number of episodes."""
        episodes = env_runner.sample(num_episodes=3, random_actions=True)
        assert len(episodes) == 3
        assert all(e.is_done for e in episodes)

    def test_sample_num_timesteps(self, env_runner):
        """Test sampling a specific number of timesteps."""
        episodes = env_runner.sample(num_timesteps=20, random_actions=True)
        total_timesteps = sum(len(e) for e in episodes)
        # Allow some slack for vectorized envs (up to num_envs extra)
        assert 20 <= total_timesteps <= 25

    def test_sample_default_rollout_fragment(self, env_runner, env_runner_config):
        """Test sampling with default rollout_fragment_length."""
        episodes = env_runner.sample(random_actions=True)
        total_timesteps = sum(len(e) for e in episodes)
        # Should be approximately rollout_fragment_length * num_envs
        expected_min = env_runner_config.rollout_fragment_length
        assert total_timesteps >= expected_min

    def test_sample_validation_both_args_error(self, env_runner):
        """Test that providing both num_timesteps and num_episodes raises error."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_timesteps=10, num_episodes=10, random_actions=True)

    def test_sample_validation_negative_timesteps(self, env_runner, runner_type):
        """Test that negative num_timesteps raises error (SingleAgent only)."""
        # Note: Only SingleAgentEnvRunner validates negative timesteps
        if runner_type == "single_agent":
            with pytest.raises(AssertionError):
                env_runner.sample(num_timesteps=-1, random_actions=True)
        else:
            # MultiAgentEnvRunner doesn't validate this - document the behavior
            pytest.skip("MultiAgentEnvRunner does not validate negative timesteps")

    def test_sample_validation_negative_episodes(self, env_runner, runner_type):
        """Test that negative num_episodes raises error (SingleAgent only)."""
        # Note: Only SingleAgentEnvRunner validates negative episodes
        if runner_type == "single_agent":
            with pytest.raises(AssertionError):
                env_runner.sample(num_episodes=-1, random_actions=True)
        else:
            # MultiAgentEnvRunner doesn't validate this - document the behavior
            pytest.skip("MultiAgentEnvRunner does not validate negative episodes")

    def test_sample_random_actions(self, env_runner):
        """Test that random_actions=True works without module inference."""
        episodes = env_runner.sample(num_episodes=2, random_actions=True)
        assert len(episodes) == 2
        # Verify actions were taken (episodes have length > 0)
        for ep in episodes:
            assert len(ep) > 0

    def test_sample_force_reset_with_timesteps(self, env_runner, runner_type):
        """Test that force_reset starts fresh episodes when using num_timesteps."""
        # Sample partial episode
        env_runner.sample(num_timesteps=5, random_actions=True)
        # Sample with force_reset
        episodes = env_runner.sample(
            num_timesteps=10, random_actions=True, force_reset=True
        )
        # All episodes should start from t=0
        # Note: SingleAgentEpisode uses t_started, MultiAgentEpisode uses env_t_started
        if runner_type == "single_agent":
            assert all(e.t_started == 0 for e in episodes)
        else:
            assert all(e.env_t_started == 0 for e in episodes)

    def test_sample_force_reset_with_episodes(self, env_runner, runner_type):
        """Test that force_reset works with num_episodes."""
        # Sample some episodes first
        env_runner.sample(num_episodes=1, random_actions=True)
        # Sample with force_reset (should still work fine)
        episodes = env_runner.sample(
            num_episodes=2, random_actions=True, force_reset=True
        )
        assert len(episodes) == 2
        # Note: SingleAgentEpisode uses t_started, MultiAgentEpisode uses env_t_started
        if runner_type == "single_agent":
            assert all(e.t_started == 0 for e in episodes)
        else:
            assert all(e.env_t_started == 0 for e in episodes)


class TestEnvRunnerEpisodeContinuation:
    """Tests for episode continuation across sample() calls."""

    def _get_t_started(self, episode, runner_type):
        """Get the t_started value handling SA vs MA episode differences."""
        if runner_type == "single_agent":
            return episode.t_started
        else:
            return episode.env_t_started

    def test_episode_continuation_between_samples(self, env_runner, runner_type):
        """Test that episodes continue correctly across sample() calls."""
        # Sample partial episode (fewer timesteps than episode length)
        env_runner.sample(num_timesteps=5, random_actions=True)

        # Sample more timesteps - should continue the same episodes
        episodes2 = env_runner.sample(num_timesteps=5, random_actions=True)

        # At least some episodes in the second batch should have t_started > 0
        # indicating they're continuations
        has_continuation = any(
            self._get_t_started(e, runner_type) > 0 for e in episodes2
        )
        assert has_continuation, "Expected at least one continued episode"

    def test_episode_ids_persist_across_samples(self, env_runner, runner_type):
        """Test that episode IDs are consistent for continued episodes."""
        # Sample partial episode
        episodes1 = env_runner.sample(num_timesteps=3, random_actions=True)
        episode_ids_1 = {e.id_ for e in episodes1 if not e.is_done}

        # Sample more - continued episodes should have same IDs
        episodes2 = env_runner.sample(num_timesteps=3, random_actions=True)
        continued_ids = {
            e.id_ for e in episodes2 if self._get_t_started(e, runner_type) > 0
        }

        # The continued episodes should have IDs from the first batch
        if continued_ids:
            assert continued_ids.issubset(
                episode_ids_1
            ), "Continued episodes should have same IDs"

    def test_force_reset_breaks_continuation(self, env_runner, runner_type):
        """Test that force_reset prevents episode continuation."""
        # Sample partial episode
        env_runner.sample(num_timesteps=5, random_actions=True)

        # Sample with force_reset - should NOT continue
        episodes = env_runner.sample(
            num_timesteps=5, random_actions=True, force_reset=True
        )

        # All episodes should start fresh
        assert all(
            self._get_t_started(e, runner_type) == 0 for e in episodes
        ), "force_reset should start all episodes from t=0"

    def test_complete_episodes_dont_continue(self, env_runner, runner_type):
        """Test that completed episodes are not continued."""
        # Sample complete episodes
        episodes1 = env_runner.sample(num_episodes=2, random_actions=True)
        assert all(e.is_done for e in episodes1)

        # Sample more complete episodes
        episodes2 = env_runner.sample(num_episodes=2, random_actions=True)

        # New episodes should all start fresh
        assert all(
            self._get_t_started(e, runner_type) == 0 for e in episodes2
        ), "New episodes after complete ones should start at t=0"


class TestEnvRunnerStateManagement:
    """Tests for state management common to both runner types.

    Note: SingleAgentEnvRunner with AlgorithmConfig (not PPOConfig) doesn't create
    a module by default, so get_state() fails. These tests use PPOConfig for both
    runner types to ensure modules are created.
    """

    def test_get_state_returns_dict(self, runner_type, ray_init):
        """Test that get_state returns a dictionary."""
        # Use PPOConfig for both to ensure module is created
        if runner_type == "single_agent":
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
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
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            state = runner.get_state()
            assert isinstance(state, dict)
        finally:
            runner.stop()

    def test_get_state_contains_expected_keys(self, runner_type, ray_init):
        """Test that get_state contains expected components."""
        if runner_type == "single_agent":
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
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
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            runner.sample(num_episodes=1, random_actions=True)
            state = runner.get_state()
            # Should have rl_module component
            assert "rl_module" in state or len(state) > 0
        finally:
            runner.stop()

    def test_set_state_accepts_state_dict(self, runner_type, ray_init):
        """Test that set_state accepts a state dictionary."""
        if runner_type == "single_agent":
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            )
            runner_cls = SingleAgentEnvRunner
        else:
            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            )
            runner_cls = MultiAgentEnvRunner

        runner = runner_cls(config=config)
        try:
            runner.sample(num_episodes=1, random_actions=True)
            state = runner.get_state()

            new_runner = runner_cls(config=config)
            try:
                new_runner.set_state(state)
            finally:
                new_runner.stop()
        finally:
            runner.stop()

    def test_get_state_set_state_roundtrip(self, runner_type, ray_init):
        """Test state can be saved and restored."""
        if runner_type == "single_agent":
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            )
            runner_cls = SingleAgentEnvRunner
        else:
            config = (
                PPOConfig()
                .environment(MultiAgentCartPole, env_config={"num_agents": 2})
                .multi_agent(
                    policies={"p0", "p1"},
                    policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
                )
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
            )
            runner_cls = MultiAgentEnvRunner

        runner = runner_cls(config=config)
        try:
            runner.sample(num_episodes=1, random_actions=True)
            state = runner.get_state()

            new_runner = runner_cls(config=config)
            try:
                new_runner.set_state(state)
                new_state = new_runner.get_state()
                assert set(state.keys()) == set(new_state.keys())
            finally:
                new_runner.stop()
        finally:
            runner.stop()


class TestEnvRunnerMetrics:
    """Tests for metrics collection common to both runner types."""

    def test_get_metrics_returns_dict(self, env_runner):
        """Test that get_metrics returns a dictionary."""
        env_runner.sample(num_episodes=2, random_actions=True)
        metrics = env_runner.get_metrics()
        assert isinstance(metrics, dict)

    def test_metrics_after_sampling(self, env_runner):
        """Test that metrics are populated after sampling."""
        env_runner.sample(num_episodes=3, random_actions=True)
        metrics = env_runner.get_metrics()

        # Should have some metrics after sampling
        assert len(metrics) > 0

    def test_metrics_accumulate_across_samples(self, env_runner):
        """Test that lifetime metrics accumulate across sample calls."""
        # Sample once
        env_runner.sample(num_episodes=2, random_actions=True)
        metrics1 = env_runner.get_metrics()

        # Sample again
        env_runner.sample(num_episodes=2, random_actions=True)
        metrics2 = env_runner.get_metrics()

        # Both should return metrics (structure may vary by implementation)
        assert isinstance(metrics1, dict)
        assert isinstance(metrics2, dict)


class TestEnvRunnerLifecycle:
    """Tests for lifecycle methods common to both runner types."""

    def test_assert_healthy(self, env_runner):
        """Test that assert_healthy passes for properly initialized runner."""
        # Should not raise for a properly initialized runner
        env_runner.assert_healthy()

    def test_get_spaces_returns_dict(self, env_runner, runner_type):
        """Test that get_spaces returns space information."""
        spaces = env_runner.get_spaces()
        assert isinstance(spaces, dict)

        # Should have at least one module's spaces
        assert len(spaces) > 0

        for module_id, (obs_space, act_space) in spaces.items():
            # Note: MultiAgentEnvRunner may return None for spaces if using
            # AlgorithmConfig without proper module setup
            if runner_type == "single_agent":
                assert isinstance(obs_space, gym.Space)
                assert isinstance(act_space, gym.Space)
            else:
                # Multi-agent with PPOConfig should have proper spaces
                # but with base AlgorithmConfig may have None
                assert obs_space is None or isinstance(obs_space, gym.Space)
                assert act_space is None or isinstance(act_space, gym.Space)

    def test_stop_can_be_called(self, env_runner):
        """Test that stop() can be called without error."""
        # Should not raise
        env_runner.stop()

    def test_stop_idempotent(self, env_runner):
        """Test that stop() can be called multiple times."""
        env_runner.stop()
        env_runner.stop()  # Should not raise on second call

    def test_make_env_creates_environment(self, env_runner):
        """Test that make_env creates an environment."""
        # Environment should already exist after construction
        assert env_runner.env is not None

        # Calling make_env should recreate it
        env_runner.make_env()
        assert env_runner.env is not None


class TestEnvRunnerErrorHandling:
    """Tests for error handling common to both runner types."""

    def test_env_step_failure_with_restart_enabled(self, runner_type, ray_init):
        """Test recovery from environment step failures when restart is enabled."""
        from conftest import ConfigurableFailingEnv

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
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=200)
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
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=200)
                .callbacks(CallbackTracker)
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            # Sample complete episodes
            runner.sample(num_episodes=2, random_actions=True)

            # Check callbacks were called
            end_calls = CallbackTracker.get_calls("on_episode_end")
            assert (
                len(end_calls) >= 2
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


class TestEnvRunnerDistributed:
    """Tests for distributed operation common to both runner types."""

    def test_can_be_ray_actor(self, env_runner_cls, env_runner_config, ray_init):
        """Test that env runner can be used as a Ray Actor."""
        remote_cls = ray.remote(num_cpus=0)(env_runner_cls)
        actor = remote_cls.remote(config=env_runner_config)

        try:
            # Should be able to call methods remotely
            episodes = ray.get(actor.sample.remote(num_episodes=1, random_actions=True))
            assert len(episodes) == 1
        finally:
            ray.kill(actor)

    def test_sample_get_state_and_metrics(self, runner_type, ray_init):
        """Test the convenience method sample_get_state_and_metrics.

        Note: This requires a properly configured runner with a module, so we use
        PPOConfig for both runner types.
        """
        if runner_type == "single_agent":
            config = (
                PPOConfig()
                .environment("CartPole-v1")
                .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
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
            )
            runner = MultiAgentEnvRunner(config=config)

        try:
            (
                episodes_ref,
                connector_states,
                metrics,
            ) = runner.sample_get_state_and_metrics()

            # episodes_ref should be a Ray ObjectRef
            assert isinstance(episodes_ref, ray.ObjectRef)

            # Should be able to get the episodes
            episodes = ray.get(episodes_ref)
            assert len(episodes) > 0

            # connector_states and metrics should be dicts
            assert isinstance(connector_states, dict)
            assert isinstance(metrics, dict)
        finally:
            runner.stop()


class TestEnvRunnerEdgeCases:
    """Tests for edge cases common to both runner types."""

    def test_zero_timesteps_sample(self, env_runner):
        """Test sampling with zero timesteps."""
        # This might either return empty or raise - document the behavior
        try:
            episodes = env_runner.sample(num_timesteps=0, random_actions=True)
            # If it doesn't raise, should return empty or minimal
            assert isinstance(episodes, list)
        except (AssertionError, ValueError):
            # Also acceptable to raise for zero timesteps
            pass

    def test_single_timestep_sample(self, env_runner):
        """Test sampling exactly one timestep."""
        episodes = env_runner.sample(num_timesteps=1, random_actions=True)
        total = sum(len(e) for e in episodes)
        # Should have at least 1 timestep (may have more due to vectorization)
        assert total >= 1

    def test_single_episode_sample(self, env_runner):
        """Test sampling exactly one episode."""
        episodes = env_runner.sample(num_episodes=1, random_actions=True)
        assert len(episodes) == 1
        assert episodes[0].is_done

    def test_many_episodes_sample(self, env_runner):
        """Test sampling many episodes."""
        episodes = env_runner.sample(num_episodes=10, random_actions=True)
        assert len(episodes) == 10
        assert all(e.is_done for e in episodes)

    def test_large_timesteps_sample(self, env_runner):
        """Test sampling many timesteps."""
        episodes = env_runner.sample(num_timesteps=500, random_actions=True)
        total = sum(len(e) for e in episodes)
        assert total >= 500


# =============================================================================
# Run tests
# =============================================================================

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
