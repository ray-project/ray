"""Shared tests for SingleAgentEnvRunner and MultiAgentEnvRunner.

These tests are parameterized to run against both runner types, ensuring
consistent behavior across the EnvRunner interface.
Additionally, the tests are split into separate classes for different testing
components.

We have attempted to use a conftest however there is a problem where bazel and pytest
view the classes as different causing some of the tests to fail.
"""
import math
from typing import Any, Optional

import gymnasium
import numpy as np
import pytest

import ray
from ray.rllib.algorithms import AlgorithmConfig, PPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils import check, override
from ray.rllib.utils.metrics import (
    EPISODE_DURATION_SEC_MEAN,
    EPISODE_LEN_MAX,
    EPISODE_LEN_MEAN,
    EPISODE_LEN_MIN,
    EPISODE_RETURN_MAX,
    EPISODE_RETURN_MEAN,
    EPISODE_RETURN_MIN,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES,
    NUM_EPISODES_LIFETIME,
)


@pytest.fixture(scope="module")
def ray_init():
    """Initialize Ray for the test module."""
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


# Parameter values for test generation
RUNNER_TYPES = ["single_agent", "multi_agent"]
NUM_ENVS_VALUES = [1, 3, 8]
GYM_VECTORIZE_MODES = [
    gymnasium.VectorizeMode.SYNC,
    gymnasium.VectorizeMode.VECTOR_ENTRY_POINT,
]


@pytest.fixture(params=RUNNER_TYPES)
def runner_type(request):
    """Fixture for runner type."""
    return request.param


@pytest.fixture(params=NUM_ENVS_VALUES)
def num_envs_per_env_runner(request):
    """Fixture for number of environments per runner."""
    return request.param


@pytest.fixture(params=GYM_VECTORIZE_MODES)
def gym_env_vectorize_mode(request):
    """Fixture for gym vectorize mode."""
    return request.param


@pytest.fixture
def env_runner_config(runner_type, num_envs_per_env_runner, gym_env_vectorize_mode):
    """Build appropriate config for each runner type."""
    # Skip invalid combinations
    if (
        runner_type == "multi_agent"
        and gym_env_vectorize_mode is gymnasium.VectorizeMode.VECTOR_ENTRY_POINT
    ):
        pytest.skip("gym_env_vectorize_mode not applicable for multi_agent")

    if runner_type == "single_agent":
        return (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_envs_per_env_runner=num_envs_per_env_runner,
                rollout_fragment_length=10,
                gym_env_vectorize_mode=gym_env_vectorize_mode,
            )
        )
    elif runner_type == "multi_agent":
        # We use MultiAgentCartPole for the parallel environment to ensure a fair comparison to the SingleAgent version.
        return (
            PPOConfig()
            .environment(MultiAgentCartPole, env_config={"num_agents": 2})
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
            )
            .env_runners(
                num_envs_per_env_runner=num_envs_per_env_runner,
                rollout_fragment_length=10,
            )
        )
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")


@pytest.fixture
def env_runner_cls(runner_type):
    """Return the appropriate EnvRunner class."""
    if runner_type == "single_agent":
        return SingleAgentEnvRunner
    elif runner_type == "multi_agent":
        return MultiAgentEnvRunner
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")


@pytest.fixture
def env_runner(env_runner_cls, env_runner_config, ray_init):
    """Create an EnvRunner instance."""
    runner = env_runner_cls(config=env_runner_config)
    yield runner
    runner.stop()


def get_t_started(episode, runner_type: str) -> int:
    """Get the t_started value handling SA vs MA episode differences."""
    if runner_type == "single_agent":
        return episode.t_started
    elif runner_type == "multi_agent":
        return episode.env_t_started
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")


class CallbackTracker(RLlibCallback):
    """Helper callback class that tracks all callback invocations."""

    # Class-level storage for callback calls
    calls: list[tuple[str, dict[str, Any]]] = []

    def on_episode_created(
        self,
        *,
        episode,
        env_runner=None,
        metrics_logger=None,
        env=None,
        env_index=None,
        rl_module=None,
        **kwargs,
    ):
        CallbackTracker.calls.append(
            ("on_episode_created", {"episode_id": episode.id_, "env_index": env_index})
        )

    def on_episode_start(
        self,
        *,
        episode,
        env_runner=None,
        metrics_logger=None,
        env=None,
        env_index=None,
        rl_module=None,
        **kwargs,
    ):
        CallbackTracker.calls.append(
            ("on_episode_start", {"episode_id": episode.id_, "env_index": env_index})
        )

    def on_episode_step(
        self,
        *,
        episode,
        env_runner=None,
        metrics_logger=None,
        env=None,
        env_index=None,
        rl_module=None,
        **kwargs,
    ):
        # Handle both SingleAgentEpisode (has .t) and MultiAgentEpisode (has .env_t)
        t_val = getattr(episode, "t", None) or getattr(episode, "env_t", None)
        CallbackTracker.calls.append(
            (
                "on_episode_step",
                {"episode_id": episode.id_, "env_index": env_index, "t": t_val},
            )
        )

    def on_episode_end(
        self,
        *,
        episode,
        env_runner=None,
        metrics_logger=None,
        env=None,
        env_index=None,
        rl_module=None,
        **kwargs,
    ):
        CallbackTracker.calls.append(
            (
                "on_episode_end",
                {
                    "episode_id": episode.id_,
                    "env_index": env_index,
                    "length": len(episode),
                },
            )
        )

    def on_sample_end(
        self,
        *,
        env_runner=None,
        metrics_logger=None,
        samples=None,
        **kwargs,
    ):
        CallbackTracker.calls.append(
            ("on_sample_end", {"num_episodes": len(samples) if samples else 0})
        )

    @classmethod
    def reset(cls):
        cls.calls = []

    @classmethod
    def get_calls(
        cls, callback_name: Optional[str] = None
    ) -> list[dict[str, Any]] | list[tuple[str, dict[str, Any]]]:
        if callback_name:
            return [c[1] for c in cls.calls if c[0] == callback_name]
        return cls.calls


@pytest.fixture
def env_runner_with_callback(runner_type, ray_init):
    CallbackTracker.reset()

    if runner_type == "single_agent":
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1)
            .callbacks(CallbackTracker)
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
            .env_runners(num_envs_per_env_runner=1)
            .callbacks(CallbackTracker)
        )
        runner = MultiAgentEnvRunner(config=config)
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")

    yield runner
    CallbackTracker.reset()
    runner.stop()


class EnvToModuleConnectorTracker(ConnectorV2):
    """Tracks all env_to_module connector calls with detailed information."""

    # Class-level storage to track calls across instances
    call_records: list[dict[str, Any]] = []
    call_count: int = 0

    def __init__(self, input_observation_space, input_action_space, **kwargs):
        super().__init__(input_observation_space, input_action_space)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module,
        batch,
        episodes: list[MultiAgentEpisode | SingleAgentEpisode],
        explore,
        shared_data,
        metrics,
        **kwargs,
    ):
        EnvToModuleConnectorTracker.call_count += 1

        for episode in episodes:
            # For SingleAgentEpisode, use .t; for MultiAgentEpisode, use .env_t
            t_val = getattr(episode, "t", None) or getattr(episode, "env_t", 0)

            record = {
                "call_number": EnvToModuleConnectorTracker.call_count,
                "episode_id": episode.id_,
                "is_done": episode.is_done,
                "is_reset": episode.is_reset if hasattr(episode, "is_reset") else None,
                "timestep": t_val,
                "explore": explore,
                "has_metrics": metrics is not None,
            }
            EnvToModuleConnectorTracker.call_records.append(record)

        return batch

    @classmethod
    def reset(cls):
        cls.call_records = []
        cls.call_count = 0

    @classmethod
    def get_records_for_episode(cls, episode_id: str) -> list[dict[str, Any]]:
        return [r for r in cls.call_records if r["episode_id"] == episode_id]

    @classmethod
    def get_done_episode_records(cls) -> list[dict[str, Any]]:
        return [r for r in cls.call_records if r["is_done"]]


class ModuleToEnvConnectorTracker(ConnectorV2):
    """Tracks all module_to_env connector calls with detailed information."""

    # Class-level storage to track calls across instances
    call_records: list[dict[str, Any]] = []
    call_count: int = 0

    def __init__(self, input_observation_space, input_action_space, **kwargs):
        super().__init__(input_observation_space, input_action_space)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module,
        batch,
        episodes: list[MultiAgentEpisode | SingleAgentEpisode],
        explore,
        shared_data,
        metrics,
        **kwargs,
    ):
        ModuleToEnvConnectorTracker.call_count += 1

        for episode in episodes:
            t_val = getattr(episode, "t", None) or getattr(episode, "env_t", 0)

            record = {
                "call_number": ModuleToEnvConnectorTracker.call_count,
                "episode_id": episode.id_,
                "is_done": episode.is_done,
                "timestep": t_val,
                "explore": explore,
                "has_batch_actions": "actions" in batch if batch else False,
            }
            ModuleToEnvConnectorTracker.call_records.append(record)

        return batch

    @classmethod
    def reset(cls):
        cls.call_records = []
        cls.call_count = 0


def make_env_to_module_connector_tracker(env, spaces, device):
    """Factory function for EnvToModuleConnectorTracker."""
    return EnvToModuleConnectorTracker(
        input_observation_space=env.observation_space,
        input_action_space=env.action_space,
    )


def make_module_to_env_connector_tracker(env, spaces):
    """Factory function for ModuleToEnvConnectorTracker."""
    return ModuleToEnvConnectorTracker(
        input_observation_space=env.observation_space if env else None,
        input_action_space=env.action_space if env else None,
    )


@pytest.fixture
def env_runner_with_env_to_module_tracker(runner_type, ray_init):
    """Create an EnvRunner with EnvToModuleConnectorTracker installed."""
    EnvToModuleConnectorTracker.reset()

    if runner_type == "single_agent":
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_envs_per_env_runner=2,
                env_to_module_connector=make_env_to_module_connector_tracker,
            )
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
            .env_runners(
                num_envs_per_env_runner=2,
                env_to_module_connector=make_env_to_module_connector_tracker,
            )
        )
        runner = MultiAgentEnvRunner(config=config)
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")

    yield runner
    EnvToModuleConnectorTracker.reset()
    runner.stop()


@pytest.fixture
def env_runner_with_module_to_env_tracker(runner_type, ray_init):
    """Create an EnvRunner with ModuleToEnvConnectorTracker installed."""
    ModuleToEnvConnectorTracker.reset()

    if runner_type == "single_agent":
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_envs_per_env_runner=1,
                module_to_env_connector=make_module_to_env_connector_tracker,
            )
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
            .env_runners(
                num_envs_per_env_runner=1,
                module_to_env_connector=make_module_to_env_connector_tracker,
            )
        )
        runner = MultiAgentEnvRunner(config=config)
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")

    yield runner
    ModuleToEnvConnectorTracker.reset()
    runner.stop()


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

    def test_sample_negative_timesteps_error(self, env_runner):
        """Test that negative num_timesteps raises error."""
        with pytest.raises(AssertionError):
            env_runner.sample(num_timesteps=-1, random_actions=True)

    def test_sample_negative_episodes_error(self, env_runner):
        """Test that negative num_episodes raises error."""
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
    """Tests for state management common to both runner types."""

    def test_get_state_returns_dict(
        self, env_runner, env_runner_config, env_runner_cls
    ):
        """Test that get_state returns a dictionary."""
        state = env_runner.get_state()
        assert isinstance(state, dict)
        assert "rl_module" in state

        env_runner.sample(num_episodes=1, random_actions=True)

        # recheck after sample
        state = env_runner.get_state()
        assert isinstance(state, dict)
        assert "rl_module" in state

        # check that a new env runner can be updated based on an older state
        new_runner = env_runner_cls(config=env_runner_config)

        try:
            # Check the states are not identical
            new_state = new_runner.get_state()
            assert set(state.keys()) == set(new_state.keys())
            with pytest.raises(
                AssertionError, match="Arrays are not almost equal to 5 decimal"
            ):
                check(state, new_state)

            new_runner.set_state(state)

            # roundtrip the runner state
            new_state = new_runner.get_state()
            assert set(state.keys()) == set(new_state.keys())
            check(state, new_state)
        finally:
            new_runner.stop()


class TestEnvRunnerMetrics:
    """Tests for metrics collection common to both runner types.

    Both SingleAgentEnvRunner and MultiAgentEnvRunner share a common metrics
    interface via `get_metrics()`. This test class verifies that:
    1. Metrics are properly initialized and returned as dicts
    2. Core metrics keys (env steps, episodes, returns) exist in both
    3. Episode metrics are only logged after completed episodes
    4. Metrics accumulate correctly over multiple sample calls
    5. Metrics are properly cleared after get_metrics() is called
    """

    # Shared metrics keys that should exist in both runner types
    SHARED_STEP_METRICS = [
        NUM_ENV_STEPS_SAMPLED,
        NUM_ENV_STEPS_SAMPLED_LIFETIME,
    ]

    SHARED_EPISODE_COUNT_METRICS = [NUM_EPISODES, NUM_EPISODES_LIFETIME]

    SHARED_EPISODE_STATS_METRICS = [
        EPISODE_LEN_MAX,
        EPISODE_LEN_MIN,
        EPISODE_LEN_MEAN,
        EPISODE_DURATION_SEC_MEAN,
        EPISODE_RETURN_MEAN,
        EPISODE_RETURN_MAX,
        EPISODE_RETURN_MIN,
    ]

    def test_get_metrics_returns_dict(self, env_runner):
        """Test that get_metrics returns a dictionary."""
        env_runner.sample(num_episodes=1, random_actions=True)
        metrics = env_runner.get_metrics()
        assert isinstance(metrics, dict)
        assert set(
            self.SHARED_STEP_METRICS
            + self.SHARED_EPISODE_COUNT_METRICS
            + self.SHARED_EPISODE_STATS_METRICS
        ) <= set(metrics.keys())

    def test_metrics_after_sampling_timesteps(self, env_runner, num_timesteps=100):
        """Test that step metrics exist after sampling timesteps."""
        episodes = env_runner.sample(num_timesteps=num_timesteps, random_actions=True)
        metrics = env_runner.get_metrics()

        # Check step metrics exist
        for key in self.SHARED_STEP_METRICS:
            assert key in metrics, f"Missing metric: {key}"

        # Verify env steps count
        max_num_timesteps = (
            math.ceil(num_timesteps / env_runner.num_envs) * env_runner.num_envs
        )
        assert num_timesteps <= metrics[NUM_ENV_STEPS_SAMPLED] <= max_num_timesteps
        assert (
            num_timesteps
            <= metrics[NUM_ENV_STEPS_SAMPLED_LIFETIME]
            <= max_num_timesteps
        )
        num_completed_episodes = sum(eps.is_done for eps in episodes)
        if num_completed_episodes > 0:
            assert metrics[NUM_EPISODES] == num_completed_episodes
            assert metrics[EPISODE_LEN_MEAN] > 0
            # CartPole return is always positive
            assert metrics[EPISODE_RETURN_MEAN] > 0
            assert metrics[EPISODE_DURATION_SEC_MEAN] > 0

    def test_metrics_after_sampling_rollout_fragment(self, env_runner):
        """Test that step metrics exist after sampling timesteps."""
        episodes = env_runner.sample(random_actions=True)
        metrics = env_runner.get_metrics()

        # Check step metrics exist
        for key in self.SHARED_STEP_METRICS:
            assert key in metrics, f"Missing metric: {key}"

        # Verify env steps count
        expected_num_timesteps = sum(len(eps) for eps in episodes)
        assert metrics[NUM_ENV_STEPS_SAMPLED] == expected_num_timesteps
        assert metrics[NUM_ENV_STEPS_SAMPLED_LIFETIME] == expected_num_timesteps
        num_completed_episodes = sum(eps.is_done for eps in episodes)
        if num_completed_episodes > 0:
            assert metrics[NUM_EPISODES] == num_completed_episodes
            assert metrics[EPISODE_LEN_MEAN] > 0
            # CartPole return is always positive
            assert metrics[EPISODE_RETURN_MEAN] > 0
            assert metrics[EPISODE_DURATION_SEC_MEAN] > 0

    def test_metrics_after_sampling_episodes(self, env_runner, num_episodes=2):
        """Test that episode metrics exist after sampling complete episodes."""
        episodes = env_runner.sample(num_episodes=num_episodes, random_actions=True)
        metrics = env_runner.get_metrics()

        # Check episode count metrics
        for key in self.SHARED_EPISODE_COUNT_METRICS:
            assert key in metrics, f"Missing metric: {key}"

        # With multiple environments, if on the same timestep that the final episode is collected,
        #   then other environment can also terminate causing greater than the number of episodes requested
        assert metrics[NUM_EPISODES] >= num_episodes
        assert metrics[NUM_EPISODES_LIFETIME] >= num_episodes
        episode_num_timesteps = sum(len(eps) for eps in episodes)
        # As some sub-environment stepped but didn't complete the episode, more steps might have been sampled than returned.
        assert metrics[NUM_ENV_STEPS_SAMPLED] >= episode_num_timesteps
        assert metrics[NUM_ENV_STEPS_SAMPLED_LIFETIME] >= episode_num_timesteps

        # Check episode stats metrics exist after complete episodes
        for key in self.SHARED_EPISODE_STATS_METRICS:
            assert key in metrics, f"Missing metric: {key}"

        # Episode return and length should be positive
        assert metrics[EPISODE_LEN_MEAN] > 0
        # CartPole return is always positive
        assert metrics[EPISODE_RETURN_MEAN] > 0
        assert metrics[EPISODE_DURATION_SEC_MEAN] > 0

    def test_metrics_accumulate_over_samples(self, env_runner):
        """Test that metrics accumulate correctly over multiple sample calls.

        As an env-runner metrics isn't root (algorithm will be), then lifetime metrics
        aren't aggregated over multiple samples.
        """
        # Zero sample
        metrics_0 = env_runner.get_metrics()
        assert metrics_0 == {}

        # First sample
        episodes_1 = env_runner.sample(num_episodes=1, random_actions=True)
        metrics_1 = env_runner.get_metrics()
        steps_sampled_1 = metrics_1[NUM_ENV_STEPS_SAMPLED]
        lifetime_1 = metrics_1[NUM_ENV_STEPS_SAMPLED_LIFETIME]
        episodes_lifetime_1 = metrics_1[NUM_EPISODES_LIFETIME]
        assert steps_sampled_1 >= sum(len(eps) for eps in episodes_1)
        assert steps_sampled_1 >= lifetime_1
        # on the final timestep sampled, if other environment also terminate then
        # they will count towards
        assert episodes_lifetime_1 >= sum(eps.is_done for eps in episodes_1)

        # Second sample
        episodes_2 = env_runner.sample(num_episodes=1, random_actions=True)
        metrics_2 = env_runner.get_metrics()
        steps_sampled_2 = metrics_2[NUM_ENV_STEPS_SAMPLED]
        lifetime_2 = metrics_2[NUM_ENV_STEPS_SAMPLED_LIFETIME]
        episodes_lifetime_2 = metrics_2[NUM_EPISODES_LIFETIME]
        assert steps_sampled_2 >= sum(len(eps) for eps in episodes_2)
        assert steps_sampled_2 >= lifetime_2
        assert episodes_lifetime_2 >= sum(eps.is_done for eps in episodes_2)

    def test_metrics_cleared_after_get_metrics(self, env_runner):
        """Test that per-iteration metrics are cleared after get_metrics."""
        # Sample some episodes
        env_runner.sample(num_episodes=2, random_actions=True)
        env_runner.get_metrics()

        # Get metrics again without sampling
        metrics = env_runner.get_metrics()
        assert np.isnan(metrics[NUM_ENV_STEPS_SAMPLED].peek())
        assert metrics[NUM_ENV_STEPS_SAMPLED_LIFETIME] == 0.0
        assert np.isnan(metrics[NUM_EPISODES].peek())
        assert metrics[NUM_EPISODES_LIFETIME] == 0.0

    def test_metrics_min_max_tracking(self, env_runner):
        """Test that min/max episode metrics are tracked correctly."""
        # Sample multiple episodes to get variation
        env_runner.sample(num_episodes=5, random_actions=True)
        metrics = env_runner.get_metrics()

        # Min should be <= mean <= max for episode length
        assert metrics[EPISODE_LEN_MIN] <= metrics[EPISODE_LEN_MEAN]
        assert metrics[EPISODE_LEN_MEAN] <= metrics[EPISODE_LEN_MAX]

        # Min should be <= mean <= max for episode return
        assert metrics[EPISODE_RETURN_MIN] <= metrics[EPISODE_RETURN_MEAN]
        assert metrics[EPISODE_RETURN_MEAN] <= metrics[EPISODE_RETURN_MAX]

    def test_metrics_consistency_across_sample_modes(self, env_runner):
        """Test that metrics structure is consistent regardless of sample mode."""
        # Sample by timesteps
        env_runner.sample(num_timesteps=20, random_actions=True, force_reset=True)
        metrics_timesteps = env_runner.get_metrics()

        # Sample by episodes
        env_runner.sample(num_episodes=1, random_actions=True, force_reset=True)
        metrics_episodes = env_runner.get_metrics()

        # Core step metrics should exist in both
        for key in self.SHARED_STEP_METRICS:
            assert key in metrics_timesteps, f"Missing in timesteps mode: {key}"
            assert key in metrics_episodes, f"Missing in episodes mode: {key}"


class TestEnvRunnerCallbacks:
    """Tests for callback invocations common to both runner types.

    Possible callbacks: on_episode_created, on_episode_start, on_episode_step, on_episode_end, on_sample_end
    """

    @pytest.mark.parametrize("num_timesteps", [8, 32])
    def test_callbacks_on_sample_timesteps(
        self, env_runner_with_callback, ray_init, num_timesteps
    ):
        """Test the callbacks for sample timesteps."""
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
        assert on_sample_end_calls[0][NUM_EPISODES] == len(episodes)

    @pytest.mark.parametrize("num_episodes", [1, 8])
    def test_callbacks_on_sample_episodes(
        self, env_runner_with_callback, ray_init, num_episodes
    ):
        """Test the callbacks for completed episodes.

        When sampling by num_episodes, the runner skips creating/starting a new
        episode after the final episode completes (since it would never be used).
        So we expect exactly num_episodes created/started calls.
        """
        episodes = env_runner_with_callback.sample(
            num_episodes=num_episodes, random_actions=True
        )

        on_episode_created_calls = CallbackTracker.get_calls("on_episode_created")
        on_episode_start_calls = CallbackTracker.get_calls("on_episode_start")
        on_episode_end_calls = CallbackTracker.get_calls("on_episode_end")
        on_sample_end_calls = CallbackTracker.get_calls("on_sample_end")

        # When sampling by num_episodes, the runner skips creating a new episode
        # after the final episode completes, so we expect exactly num_episodes calls
        assert len(on_episode_created_calls) == num_episodes + 1
        assert len(on_episode_start_calls) == num_episodes
        assert len(on_episode_end_calls) == num_episodes == len(episodes)
        assert len(on_sample_end_calls) == 1
        assert on_sample_end_calls[0][NUM_EPISODES] == num_episodes

    def test_callbacks_on_sample_rollout(self, env_runner_with_callback, ray_init):
        """Test the callbacks for sampling with default rollout fragment."""
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
        assert on_sample_end_calls[0][NUM_EPISODES] == len(episodes)

    @pytest.mark.parametrize("num_episodes", [1, 8])
    def test_callbacks_multi_samples(
        self, env_runner_with_callback, ray_init, num_episodes, repeats=3
    ):
        """Test callbacks across multiple sample() calls.

        When sampling by num_episodes, the runner skips creating a new episode
        after the final episode completes. Each sample() call independently
        creates exactly num_episodes episodes.
        """
        for repeat in range(repeats):
            episodes = env_runner_with_callback.sample(
                num_episodes=num_episodes, random_actions=True
            )
            assert len(episodes) == num_episodes

            on_episode_created_calls = CallbackTracker.get_calls("on_episode_created")
            on_episode_start_calls = CallbackTracker.get_calls("on_episode_start")
            on_episode_end_calls = CallbackTracker.get_calls("on_episode_end")
            on_sample_end_calls = CallbackTracker.get_calls("on_sample_end")

            # Cumulative counts: each sample() creates num_episodes episodes
            expected_created = (num_episodes + 1) * (repeat + 1)
            expected_started = num_episodes * (repeat + 1)
            expected_ended = num_episodes * (repeat + 1)

            assert len(on_episode_created_calls) == expected_created
            assert len(on_episode_start_calls) == expected_started
            assert len(on_episode_end_calls) == expected_ended
            assert len(on_sample_end_calls) == repeat + 1
            assert on_sample_end_calls[-1][NUM_EPISODES] == num_episodes


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

        records = EnvToModuleConnectorTracker.call_records
        assert len(records) == 0

        # Initial reset happens during construction, sample triggers it
        env_runner.sample(num_timesteps=0, random_actions=True)

        # Should have records for each vectorized env after reset
        assert len(records) == env_runner.num_envs

        # First records should be at timestep 0 (reset)
        reset_records = [r for r in records if r["timestep"] == 0]
        assert len(reset_records) == env_runner.num_envs

    @pytest.mark.parametrize("num_timesteps", [8, 25, 50, 100])
    def test_env_to_module_called_per_step(
        self, env_runner_with_env_to_module_tracker, num_timesteps
    ):
        """Test env_to_module connector is called after each environment step."""
        env_runner = env_runner_with_env_to_module_tracker

        env_runner.sample(num_timesteps=num_timesteps, random_actions=True)

        # Connector is called once per loop iteration, not once per timestep
        # With vectorized envs, each iteration steps all envs in parallel
        # So: 1 reset call + ceil(num_timesteps / num_envs) step calls
        call_count = EnvToModuleConnectorTracker.call_count

        min_expected_calls = 1 + math.ceil(num_timesteps / env_runner.num_envs)
        assert call_count >= min_expected_calls

    @pytest.mark.parametrize("num_timesteps", [8, 25, 50, 100])
    def test_module_to_env_called_only_with_rl_module(
        self, env_runner_with_module_to_env_tracker, num_timesteps
    ):
        """Test module_to_env connector is called only when RLModule is used.

        Verifies:
        1. module_to_env IS called when using random_actions=False (RLModule engaged)
        2. module_to_env is NOT called when using random_actions=True (RLModule bypassed)
        """
        env_runner = env_runner_with_module_to_env_tracker

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

    def test_env_to_module_postprocess_done_episodes_multi_agent(
        self, env_runner_with_env_to_module_tracker, runner_type
    ):
        """Test that MultiAgent runner calls env_to_module for done episode postprocessing.

        This is specific to MultiAgentEnvRunner which has an extra connector call
        for done episodes to postprocess artifacts like one-hot encoded observations.
        """
        if runner_type != "multi_agent":
            pytest.skip("Test only applicable to multi_agent runner")

        env_runner = env_runner_with_env_to_module_tracker
        num_episodes = 3

        episodes = env_runner.sample(num_episodes=num_episodes, random_actions=True)
        # With multiple envs, we may get more than num_episodes due to parallel completion
        assert len(episodes) >= num_episodes

        # Check that done episodes were recorded
        done_records = EnvToModuleConnectorTracker.get_done_episode_records()
        # Each done episode should have at least one record where is_done=True
        assert len(done_records) >= num_episodes


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
