from collections import defaultdict
from itertools import chain

import gymnasium as gym

from ray.rllib.core.rl_module import MultiRLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.metrics import (
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES,
    NUM_EPISODES_LIFETIME,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
)

from rllib.env.external.base_external_env_runner_server import BaseExternalEnvRunnerServer
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import get_device


@DeveloperAPI
class MultiAgentEnvRunnerServerForExternalInference(
    BaseExternalEnvRunnerServer[MultiAgentEpisode]
):
    """Multi-agent EnvRunner that receives episodes from an external client via TCP.

    Assumptions:
    - One external client connects via TCP.
    - The client owns and runs the RLModule (e.g., as ONNX) and all connectors.
    - Episodes are sent in bulk as lists of `MultiAgentEpisode` states.
    - This server keeps a local `MultiRLModule` only for weights synchronization.
    """

    @property
    def episode_type(self) -> type[MultiAgentEpisode]:
        return MultiAgentEpisode

    @property
    def base_env_runner_type(self) -> type[EnvRunner]:
        return MultiAgentEnvRunner

    def _increase_sampled_metrics(
        self, num_env_steps: int, num_episodes_completed: int
    ) -> None:
        agent_steps, module_steps = defaultdict(int), defaultdict(int)
        for eps in chain(
            self._done_episodes_for_metrics,
            *self._ongoing_episodes_for_metrics.values()
        ):
            for aid, sa_eps in eps.agent_episodes.items():
                agent_steps[str(aid)] += len(sa_eps)
                module_steps[sa_eps.module_id] += len(sa_eps)

        self.metrics.log_value(
            NUM_ENV_STEPS_SAMPLED, num_env_steps, reduce="sum",
            clear_on_reduce=True
        )
        self.metrics.log_value(
            NUM_ENV_STEPS_SAMPLED_LIFETIME,
            num_env_steps,
            reduce="sum",
            with_throughput=True,
        )
        self.metrics.log_value(
            NUM_EPISODES, num_episodes_completed, reduce="sum", clear_on_reduce=True
        )
        self.metrics.log_value(
            NUM_EPISODES_LIFETIME, num_episodes_completed, reduce="sum"
        )

        # Record agent and module metrics.
        for aid, steps in agent_steps.items():
            self.metrics.log_value(
                (NUM_AGENT_STEPS_SAMPLED, str(aid)),
                steps,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                (NUM_AGENT_STEPS_SAMPLED_LIFETIME, str(aid)),
                steps,
                reduce="sum",
            )
        for mid, steps in module_steps.items():
            self.metrics.log_value(
                (NUM_MODULE_STEPS_SAMPLED, str(mid)),
                steps,
                reduce="sum",
                clear_on_reduce=True,
            )
            self.metrics.log_value(
                (NUM_MODULE_STEPS_SAMPLED_LIFETIME, str(mid)),
                steps,
                reduce="sum",
            )

    @override(EnvRunner)
    def make_module(self) -> None:
        module_spec: MultiRLModuleSpec = self.config.get_multi_rl_module_spec(
            env=None, spaces=self.get_spaces(), inference_only=True
        )
        self.module = module_spec.build()

    @override(EnvRunner)
    def get_spaces(self) -> dict[str, tuple[gym.Space, gym.Space]]:
        env_to_module_pipeline_for_spaces = self.config.build_env_to_module_connector(
            env=None, spaces=None, device=get_device(self.config, 0)
        )
        return {
            INPUT_ENV_SPACES: (self.config.observation_space, self.config.action_space),
            **{
                mid: (o, env_to_module_pipeline_for_spaces.action_space[mid])
                for mid, o in
                env_to_module_pipeline_for_spaces.observation_space.spaces.items()
            },
        }
