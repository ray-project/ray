import abc
import ray

from typing import Any, Dict, List, Set, Tuple, Union

from ray import ObjectRef
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import (
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
)
from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample

# TODO (simon): Maybe rename after old stack deprecation to `MetricsDict`.
from ray.rllib.utils.typing import ResultDict


class EnvRunnerMixin(abc.ABC):
    def __init__(self, config: AlgorithmConfig, *args, **kwargs):
        # The `EnvRunnerGroup` could be customized.
        self._env_runner_group: EnvRunnerGroup = None
        # Also enable a local `EnvRunner`.
        self._local_env_runner: Union[MultiAgentEnvRunner, SingleAgentEpisode] = None
        # Setup the environment runners.
        # self.setup(config=config)

    abc.abstractmethod

    def _setup(self, config: AlgorithmConfig):
        """Abstract method to sertup the specific `EnvRunner`s."""
        pass

    abc.abstractmethod

    def sample(
        self, env_steps: int, agent_steps: int
    ) -> Tuple[List[Union[MultiAgentEpisode, SingleAgentEpisode]], ResultDict]:
        """Samples experiences from `EnvRunner`s."""
        pass

    abc.abstractmethod

    def sync(self, state):
        """Syncs states between `EnvRunner`s and `RLAgorithm`."""
        self._env_runner_group.sync_weights(state)
        self._env_runner_group.sync_env_runner_states(state)

    abc.abstractmethod

    def cleanup(self):
        """Pulls down all `EnvRunner`s."""
        self._env_runner_group.stop()
        self._env_runner_group = None
        self._local_env_runner = None


class SyncEnvRunnerConcreteMixin(EnvRunnerMixin):
    class SyncEnvRunnerGroup(EnvRunnerGroup):
        def sample(
            self, env_steps: int, agent_steps: int
        ) -> Tuple[List[Union[MultiAgentEpisode, SingleAgentEpisode]], ResultDict]:
            """Samples synchronously from `EnvRunner`s."""
            # TODO (simon): Move logic from function to here.
            return synchronous_parallel_sample(
                self._env_runner_group,
                max_agent_steps=agent_steps,
                max_env_steps=env_steps,
                sample_timeout_s=self._env_runner_group._local_config.sample_timeout_s,
                concat=False,
                _uses_new_env_runners=True,
                _return_metrics=False,
            )

    @override(EnvRunnerMixin)
    def _setup(self, config: AlgorithmConfig):
        print("Setup SynvEnvRunnerMixin ... ")
        self._env_runner_group = self.SyncEnvRunnerGroup(config)
        if config.create_local_env_runner:
            self._local_env_runner = (
                MultiAgentEnvRunner(config)
                if config.is_multi_agent
                else SingleAgentEnvRunner(config)
            )
        # ...

    @override(EnvRunnerMixin)
    def sample(
        self, env_steps: int, agent_steps: int
    ) -> Tuple[List[Union[MultiAgentEpisode, SingleAgentEpisode]], ResultDict]:
        return self._env_runner_group.sample(env_steps, agent_steps)


class AsyncEnvRunnerConcreteMixin(EnvRunnerMixin):
    class AsyncEnvRunnerGroup(EnvRunnerGroup):
        def sample(
            self, env_steps: int, agent_steps: int
        ) -> Tuple[List[ObjectRef], Dict[str, Any], Dict[str, Any], Set[int]]:

            env_runner_indices_to_update = set()
            num_healthy_remote_workers = (
                self._env_runner_group.num_healthy_remote_workers()
            )
            if num_healthy_remote_workers > 0:
                async_results: List[
                    Tuple[int, ObjectRef]
                ] = self.env_runner_group.fetch_ready_async_reqs(
                    timeout_seconds=self.config.timeout_s_sampler_manager,
                    return_obj_refs=False,
                )
                self.env_runner_group.foreach_env_runner_async(
                    "sample_get_state_and_metrics"
                )

                # Get results from the n different async calls and store those EnvRunner
                # indices we should update.
                results = []
                for r in async_results:
                    env_runner_indices_to_update.add(r[0])
                    results.append(r[1])

                for (episodes, states, metrics) in results:
                    episode_refs.append(episodes)
                    connector_states.append(states)
                    env_runner_metrics.append(metrics)
            # Sample from the local EnvRunner.
            else:
                episodes = self._local_env_runner.sample()
                env_runner_metrics = [self._local_env_runner.get_metrics()]
                episode_refs = [ray.put(episodes)]
                connector_states = [
                    self.env_runner.get_state(
                        components=[
                            COMPONENT_ENV_TO_MODULE_CONNECTOR,
                            COMPONENT_MODULE_TO_ENV_CONNECTOR,
                        ]
                    )
                ]

            return (
                episode_refs,
                connector_states,
                env_runner_metrics,
                env_runner_indices_to_update,
            )

    @override(EnvRunnerMixin)
    def _setup(self, config: AlgorithmConfig):
        self._env_runner_group = self.AsyncEnvRunnerGroup(config)
        # To cover for failed remote workers this mixin always provides
        # the local `EnvRunner`.
        self._local_env_runner = (
            MultiAgentEnvRunner(config)
            if config.is_multi_agent
            else SingleAgentEnvRunner(config)
        )
        # ...

    @override(EnvRunnerMixin)
    def sample(self, env_steps: int, agent_steps: int) -> List[SingleAgentEpisode]:
        return self._env_runner_group.sample(env_steps, agent_steps)
