"""Shared tests for SingleAgentEnvRunner and MultiAgentEnvRunner.

These tests are parameterized to run against both runner types, ensuring
consistent behavior across the EnvRunner interface.
Additionally, the tests are split into separate classes for different testing
components.
"""
import math

import pytest
from conftest import (
    CallbackTracker,
    EnvToModuleConnectorTracker,
    ModuleToEnvConnectorTracker,
    get_t_started,
    make_env_to_module_connector_tracker,
)

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
        """Test sampling a number of timesteps."""
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
            rollout_fragment_length = env_runner_config.rollout_fragment_length
            assert (
                env_runner.num_envs * rollout_fragment_length
                <= total_timesteps
                <= (env_runner.num_envs * rollout_fragment_length + env_runner.num_envs)
            )

    def test_sample_force_reset_with_timesteps(self, env_runner, runner_type):
        """Test that force_reset starts fresh episodes when using num_timesteps."""
        for repeat in range(self.repeats):
            # Sample partial episode
            env_runner.sample(num_timesteps=5, random_actions=True)
            # Sample with force_reset
            episodes = env_runner.sample(
                num_timesteps=10, random_actions=True, force_reset=True
            )

            assert all(get_t_started(e, runner_type) == 0 for e in episodes)

    def test_sample_force_reset_with_episodes(self, env_runner, runner_type):
        """Test that force_reset works with num_episodes."""
        # Sample some episodes first
        env_runner.sample(num_episodes=1, random_actions=True)
        # Sample with force_reset (should still work fine)
        episodes = env_runner.sample(
            num_episodes=2, random_actions=True, force_reset=True
        )
        assert len(episodes) == 2
        assert all(get_t_started(e, runner_type) == 0 for e in episodes)

    def test_sample_zero_timesteps(self, env_runner):
        """Test sampling with zero timesteps."""
        # This might either return empty or raise - document the behavior
        episodes = env_runner.sample(num_timesteps=0, random_actions=True)
        # If it doesn't raise, should return empty or minimal
        assert isinstance(episodes, list)
        assert len(episodes) == 0

    def test_sample_zero_episodes(self, env_runner):
        """Test sampling with zero timesteps."""
        # This might either return empty or raise - document the behavior
        episodes = env_runner.sample(num_episodes=0, random_actions=True)
        # If it doesn't raise, should return empty or minimal
        assert isinstance(episodes, list)
        assert len(episodes) == 0

    def test_sample_both_args_error(self, env_runner):
        """Test that providing both num_timesteps and num_episodes raises error."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_timesteps=10, num_episodes=10, random_actions=True)

    def test_sample_negative_timesteps_error(self, env_runner, runner_type):
        """Test that negative num_timesteps raises error (SingleAgent only)."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_timesteps=-1, random_actions=True)

    def test_sample_negative_episodes_error(self, env_runner, runner_type):
        """Test that negative num_episodes raises error (SingleAgent only)."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_episodes=-1, random_actions=True)


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

            # Check the states are not identical
            new_state = new_runner.get_state()
            assert set(state.keys()) == set(new_state.keys())
            assert new_state != state

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

    # TODO after https://github.com/ray-project/ray/pull/56838 merged


class TestEnvRunnerErrorHandling:
    """Tests for error handling common to both runner types."""

    # TODO


class TestEnvRunnerCallbacks:
    """Tests for callback invocations common to both runner types.

    Possible callbacks: on_episode_created, on_episode_start, on_episode_step, on_episode_end, on_sample_end
    """

    @pytest.mark.parametrize("num_timesteps", [8, 32])
    def test_callbacks_on_sample_timesteps(
        self, env_runner_with_callback, ray_init, num_timesteps
    ):
        """Test the callbacks for sample timesteps."""
        CallbackTracker.reset()

        try:
            # Sample complete episodes
            episodes = env_runner_with_callback.sample(
                num_timesteps=num_timesteps, random_actions=True
            )

            on_episode_created_calls = CallbackTracker.get_calls("on_episode_created")
            on_episode_start_calls = CallbackTracker.get_calls("on_episode_start")
            on_episode_end_calls = CallbackTracker.get_calls("on_episode_end")
            on_sample_end_calls = CallbackTracker.get_calls("on_sample_end")

            assert (
                len(on_episode_created_calls)
                == sum(e.is_done for e in episodes) + env_runner_with_callback.num_envs
            )
            assert (
                len(on_episode_start_calls)
                == sum(e.is_done for e in episodes) + env_runner_with_callback.num_envs
            )
            assert len(on_episode_end_calls) == sum(e.is_done for e in episodes)
            assert len(on_sample_end_calls) == 1
            assert on_sample_end_calls[0]["num_episodes"] == len(episodes)
        finally:
            env_runner_with_callback.stop()
            CallbackTracker.reset()

    @pytest.mark.parametrize("num_episodes", [1, 8])
    def test_callbacks_on_sample_episodes(
        self, env_runner_with_callback, ray_init, num_episodes
    ):
        """Test the callbacks for completed episodes.

        When sampling by num_episodes, the runner skips creating/starting a new
        episode after the final episode completes (since it would never be used).
        So we expect exactly num_episodes created/started calls.
        """
        CallbackTracker.reset()

        try:
            # Sample complete episodes
            episodes = env_runner_with_callback.sample(
                num_episodes=num_episodes, random_actions=True
            )

            on_episode_created_calls = CallbackTracker.get_calls("on_episode_created")
            on_episode_start_calls = CallbackTracker.get_calls("on_episode_start")
            on_episode_end_calls = CallbackTracker.get_calls("on_episode_end")
            on_sample_end_calls = CallbackTracker.get_calls("on_sample_end")

            # When sampling by num_episodes, the runner skips creating a new episode
            # after the final episode completes, so we expect exactly num_episodes calls
            assert len(on_episode_created_calls) == num_episodes
            assert len(on_episode_start_calls) == num_episodes
            assert len(on_episode_end_calls) == num_episodes == len(episodes)
            assert len(on_sample_end_calls) == 1
            assert on_sample_end_calls[0]["num_episodes"] == num_episodes
        finally:
            env_runner_with_callback.stop()
            CallbackTracker.reset()

    def test_callbacks_on_sample_rollout(self, env_runner_with_callback, ray_init):
        CallbackTracker.reset()

        try:
            # Sample complete episodes
            episodes = env_runner_with_callback.sample(random_actions=True)

            on_episode_created_calls = CallbackTracker.get_calls("on_episode_created")
            on_episode_start_calls = CallbackTracker.get_calls("on_episode_start")
            on_episode_end_calls = CallbackTracker.get_calls("on_episode_end")
            on_sample_end_calls = CallbackTracker.get_calls("on_sample_end")

            assert (
                len(on_episode_created_calls)
                == sum(e.is_done for e in episodes) + env_runner_with_callback.num_envs
            )
            assert (
                len(on_episode_start_calls)
                == sum(e.is_done for e in episodes) + env_runner_with_callback.num_envs
            )
            assert len(on_episode_end_calls) == sum(e.is_done for e in episodes)
            assert len(on_sample_end_calls) == 1
            assert on_sample_end_calls[0]["num_episodes"] == len(episodes)
        finally:
            env_runner_with_callback.stop()
            CallbackTracker.reset()

    @pytest.mark.parametrize("num_episodes", [1, 8])
    def test_callbacks_multi_samples(
        self, env_runner_with_callback, ray_init, num_episodes, repeats=3
    ):
        """Test callbacks across multiple sample() calls.

        When sampling by num_episodes, the runner skips creating a new episode
        after the final episode completes. Each sample() call independently
        creates exactly num_episodes episodes.
        """
        CallbackTracker.reset()

        try:
            for repeat in range(repeats):
                # Sample complete episodes
                episodes = env_runner_with_callback.sample(
                    num_episodes=num_episodes, random_actions=True
                )
                assert len(episodes) == num_episodes

                on_episode_created_calls = CallbackTracker.get_calls(
                    "on_episode_created"
                )
                on_episode_start_calls = CallbackTracker.get_calls("on_episode_start")
                on_episode_end_calls = CallbackTracker.get_calls("on_episode_end")
                on_sample_end_calls = CallbackTracker.get_calls("on_sample_end")

                # Cumulative counts: each sample() creates num_episodes episodes
                expected_created = num_episodes * (repeat + 1)
                expected_started = num_episodes * (repeat + 1)
                expected_ended = num_episodes * (repeat + 1)

                assert len(on_episode_created_calls) == expected_created
                assert len(on_episode_start_calls) == expected_started
                assert len(on_episode_end_calls) == expected_ended
                assert len(on_sample_end_calls) == repeat + 1
                assert on_sample_end_calls[-1]["num_episodes"] == num_episodes
        finally:
            env_runner_with_callback.stop()
            CallbackTracker.reset()


class TestEnvRunnerConnectors:
    """Tests for connector invocations in both runner types.

    Connectors are called in specific situations:

    env_to_module connector:
    - After environment reset (to process initial observations)
    - After each environment step (to process observations for next action)
    - For done episodes in MultiAgent (extra postprocessing call)

    module_to_env connector:
    - After RLModule forward pass (to process actions before sending to env)
    - NOT called when using random_actions=True
    """

    def test_env_to_module_called_on_reset(self, env_runner_with_env_to_module_tracker):
        """Test env_to_module connector is called during environment reset."""
        env_runner = env_runner_with_env_to_module_tracker

        # Initial reset happens during construction, sample triggers it
        env_runner.sample(num_timesteps=1, random_actions=True)

        # Should have records for each vectorized env after reset
        records = EnvToModuleConnectorTracker.call_records
        assert len(records) >= env_runner.num_envs

        # First records should be at timestep 0 (reset)
        reset_records = [r for r in records if r["timestep"] == 0]
        assert len(reset_records) >= env_runner.num_envs

    def test_env_to_module_called_per_step(self, env_runner_with_env_to_module_tracker):
        """Test env_to_module connector is called after each environment step."""
        env_runner = env_runner_with_env_to_module_tracker
        num_timesteps = 5

        env_runner.sample(num_timesteps=num_timesteps, random_actions=True)

        # Connector is called once per loop iteration, not once per timestep
        # With vectorized envs, each iteration steps all envs in parallel
        # So: 1 reset call + ceil(num_timesteps / num_envs) step calls
        call_count = EnvToModuleConnectorTracker.call_count

        min_expected_calls = 1 + math.ceil(num_timesteps / env_runner.num_envs)
        assert call_count >= min_expected_calls

    def test_env_to_module_postprocess_done_episodes_multi_agent(self, ray_init):
        """Test that MultiAgent runner calls env_to_module for done episode postprocessing.

        This is specific to MultiAgentEnvRunner which has an extra connector call
        for done episodes to postprocess artifacts like one-hot encoded observations.
        """
        EnvToModuleConnectorTracker.reset()

        num_episodes = 3
        config = (
            PPOConfig()
            .environment(MultiAgentCartPole, env_config={"num_agents": 1})
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=lambda aid, *a, **kw: "p0",
            )
            .env_runners(
                num_envs_per_env_runner=1,
                env_to_module_connector=make_env_to_module_connector_tracker,
            )
        )
        env_runner = MultiAgentEnvRunner(config=config)

        try:
            episodes = env_runner.sample(num_episodes=num_episodes, random_actions=True)
            assert len(episodes) == num_episodes

            # Check that done episodes were recorded
            done_records = EnvToModuleConnectorTracker.get_done_episode_records()
            # Each done episode should have at least one record where is_done=True
            assert len(done_records) == num_episodes
        finally:
            env_runner.stop()
            EnvToModuleConnectorTracker.reset()

    def test_module_to_env_called_only_with_rl_module(
        self, env_runner_with_module_to_env_tracker
    ):
        """Test module_to_env connector is called only when RLModule is used.

        Verifies:
        1. module_to_env IS called when using random_actions=False (RLModule engaged)
        2. module_to_env is NOT called when using random_actions=True (RLModule bypassed)
        """
        env_runner = env_runner_with_module_to_env_tracker
        num_timesteps = 5

        # With random_actions=True, the RLModule is bypassed
        env_runner.sample(num_timesteps=num_timesteps, random_actions=True)
        assert ModuleToEnvConnectorTracker.call_count == 0

        # Use random_actions=False to engage the RLModule and module_to_env
        env_runner.sample(num_timesteps=num_timesteps, random_actions=False)
        assert ModuleToEnvConnectorTracker.call_count >= num_timesteps

    def test_connector_sample_options(self, env_runner_with_env_to_module_tracker):
        """Test connector behavior with various sample options.

        This test verifies:
        1. Episode IDs are consistent across sample calls (continuity)
        2. force_reset triggers new reset calls and creates new episode IDs
        3. The explore flag is correctly passed to connectors
        """
        env_runner = env_runner_with_env_to_module_tracker

        # Part 1: Test episode ID continuity across samples
        episodes_1 = env_runner.sample(num_timesteps=3, random_actions=True)
        episode_ids_1 = {e.id_ for e in episodes_1}
        call_count_after_first = EnvToModuleConnectorTracker.call_count

        # Sample more - should continue same episodes
        env_runner.sample(num_timesteps=3, random_actions=True)

        # Verify episode IDs appear in connector records
        for ep_id in episode_ids_1:
            ep_records = EnvToModuleConnectorTracker.get_records_for_episode(ep_id)
            assert len(ep_records) >= 1

        # Part 2: Test force_reset creates new episodes
        env_runner.sample(num_timesteps=5, random_actions=True, force_reset=True)
        call_count_after_reset = EnvToModuleConnectorTracker.call_count
        assert call_count_after_reset > call_count_after_first

        # Should have reset records from initial + force_reset
        records = EnvToModuleConnectorTracker.call_records
        reset_records = [r for r in records if r["timestep"] == 0]
        assert len(reset_records) >= 2 * env_runner.num_envs

        # Part 3: Test explore flag is passed correctly
        # All records so far should have explore=True (default)
        assert all(r["explore"] for r in records)

        EnvToModuleConnectorTracker.reset()

        # Sample with explore=False
        env_runner.sample(num_timesteps=3, random_actions=True, explore=False)
        records = EnvToModuleConnectorTracker.call_records
        assert all(not r["explore"] for r in records)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
