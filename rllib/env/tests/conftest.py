"""Shared fixtures for env runner tests."""
from typing import Any, Optional

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
from ray.rllib.utils import override


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


@pytest.fixture(params=[1, 3])
def num_envs_per_env_runner(request):
    """Parameterized fixture for number of environments per runner."""
    return request.param


@pytest.fixture
def env_runner_config(runner_type, num_envs_per_env_runner):
    """Build appropriate config for each runner type."""
    if runner_type == "single_agent":
        return (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(
                num_envs_per_env_runner=num_envs_per_env_runner,
                rollout_fragment_length=10,
            )
        )
    elif runner_type == "multi_agent":
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
    if runner_type == "single_agent":
        config = (
            AlgorithmConfig()
            .environment("CartPole-v1")
            .env_runners(num_envs_per_env_runner=1)
            .callbacks(CallbackTracker)
        )
        return SingleAgentEnvRunner(config=config)
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
        return MultiAgentEnvRunner(config=config)
    else:
        raise ValueError(f"Unknown runner type: {runner_type}")


class ConnectorTracker(ConnectorV2):
    """Original connector tracker for episode end counting."""

    def __init__(self, env, spaces, device):
        super().__init__(env.observation_space, env.action_space)

        self.episode_end_counter = 0
        self.episodes_encountered_list = list()
        self.episodes_encountered_set = set()

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
        if all(e.is_done for e in episodes):
            self.episode_end_counter += len(episodes)
            for episode in episodes:
                self.episodes_encountered_list.append(episode.id_)
                self.episodes_encountered_set.add(episode.id_)
        return batch


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
    runner.stop()
    EnvToModuleConnectorTracker.reset()


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
    runner.stop()
    ModuleToEnvConnectorTracker.reset()
