import gymnasium as gym

from ray.rllib.core.rl_module import MultiRLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES, INPUT_ENV_SINGLE_SPACES
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
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
        # TODO (ruben): adjust when adding multi-client setup
        return {
            INPUT_ENV_SPACES: (self.config.observation_space, self.config.action_space),
            INPUT_ENV_SINGLE_SPACES: (
                self.config.observation_space, self.config.action_space
            ),
            **{
                mid: (o, env_to_module_pipeline_for_spaces.action_space[mid])
                for mid, o in
                env_to_module_pipeline_for_spaces.observation_space.spaces.items()
            },
        }
