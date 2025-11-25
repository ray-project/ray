"""Shared fixtures for env runner tests."""
from typing import Any

import gymnasium as gym
import pytest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole


@pytest.fixture(scope="module")
def ray_init():
    """Initialize Ray for the test module."""
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture(params=["single_agent", "multi_agent"])
def runner_type(request):
    """Parameterized fixture for runner type."""
    return request.param


@pytest.fixture
def env_runner_config(runner_type):
    """Build appropriate config for each runner type."""
    if runner_type == "single_agent":
        return (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=2, rollout_fragment_length=10)
        )
    else:
        return (
            PPOConfig()
            .environment(MultiAgentCartPole, env_config={"num_agents": 2})
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
            )
            .env_runners(num_envs_per_env_runner=1, rollout_fragment_length=10)
        )


@pytest.fixture
def env_runner_cls(runner_type):
    """Return the appropriate EnvRunner class."""
    if runner_type == "single_agent":
        return SingleAgentEnvRunner
    else:
        return MultiAgentEnvRunner


@pytest.fixture
def env_runner(env_runner_cls, env_runner_config, ray_init):
    """Create an EnvRunner instance."""
    runner = env_runner_cls(config=env_runner_config)
    yield runner
    runner.stop()


# Helper environments for testing


class ImmediateTerminationEnv(gym.Env):
    """Environment that terminates immediately after one step."""

    def __init__(self, config=None):
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        return 0, {}

    def step(self, action):
        return 0, 1.0, True, False, {}


class ZeroRewardEnv(gym.Env):
    """Environment that always returns zero reward."""

    def __init__(self, config=None):
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self._step_count = 0
        self._max_steps = config.get("max_steps", 10) if config else 10

    def reset(self, *, seed=None, options=None):
        self._step_count = 0
        return 0, {}

    def step(self, action):
        self._step_count += 1
        done = self._step_count >= self._max_steps
        return 0, 0.0, done, False, {}


class NegativeRewardEnv(gym.Env):
    """Environment that always returns negative reward."""

    def __init__(self, config=None):
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self._step_count = 0
        self._max_steps = config.get("max_steps", 10) if config else 10

    def reset(self, *, seed=None, options=None):
        self._step_count = 0
        return 0, {}

    def step(self, action):
        self._step_count += 1
        done = self._step_count >= self._max_steps
        return 0, -1.0, done, False, {}


class ConfigurableFailingEnv(gym.Env):
    """Environment that fails on step or reset based on configuration."""

    # Class-level counters shared across instances
    step_count = 0
    reset_count = 0

    def __init__(self, config=None):
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        config = config or {}
        self.fail_on_step = config.get("fail_on_step", False)
        self.fail_on_reset = config.get("fail_on_reset", False)
        self.fail_after_n_steps = config.get("fail_after_n_steps", None)
        self.fail_after_n_resets = config.get("fail_after_n_resets", None)

    def reset(self, *, seed=None, options=None):
        ConfigurableFailingEnv.reset_count += 1
        if self.fail_on_reset:
            if (
                self.fail_after_n_resets is None
                or ConfigurableFailingEnv.reset_count > self.fail_after_n_resets
            ):
                raise RuntimeError("Simulated reset failure")
        return 0, {}

    def step(self, action):
        ConfigurableFailingEnv.step_count += 1
        if self.fail_on_step:
            if (
                self.fail_after_n_steps is None
                or ConfigurableFailingEnv.step_count > self.fail_after_n_steps
            ):
                raise RuntimeError("Simulated step failure")
        return 0, 1.0, False, False, {}

    @classmethod
    def reset_counters(cls):
        cls.step_count = 0
        cls.reset_count = 0


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
    def get_calls(cls, callback_name=None):
        if callback_name:
            return [c for c in cls.calls if c[0] == callback_name]
        return cls.calls
