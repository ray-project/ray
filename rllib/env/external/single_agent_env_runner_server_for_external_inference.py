import gymnasium as gym

from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.rl_module import RLModuleSpec
from ray.rllib.env import INPUT_ENV_SPACES
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.external.base_external_env_runner_server import (
    BaseExternalEnvRunnerServer,
)
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.util.annotations import DeveloperAPI
from ray.rllib.utils.annotations import override


@DeveloperAPI
class SingleAgentEnvRunnerServerForExternalInference(
    BaseExternalEnvRunnerServer[SingleAgentEpisode]
):
    """Multi-agent EnvRunner that receives episodes from an external client via TCP.

    Assumptions:
    - One external client connects via TCP.
    - The client owns and runs the RLModule (e.g., as ONNX) and all connectors.
    - Episodes are sent in bulk as lists of `MultiAgentEpisode` states.
    - This server keeps a local `MultiRLModule` only for weights synchronization.
    """

    @property
    def episode_type(self) -> type[SingleAgentEpisode]:
        return SingleAgentEpisode

    @property
    def base_env_runner_type(self) -> type[EnvRunner]:
        return SingleAgentEnvRunner

    def _increase_sampled_metrics(
        self, num_env_steps: int, num_episodes_completed: int
    ) -> None:
        SingleAgentEnvRunner._increase_sampled_metrics(
            self, num_env_steps, num_episodes_completed
        )

    @override(EnvRunner)
    def make_module(self) -> None:
        module_spec: RLModuleSpec = self.config.get_rl_module_spec(
            env=None, spaces=self.get_spaces(), inference_only=True
        )
        self.module = module_spec.build()

    @override(EnvRunner)
    def get_spaces(self) -> dict[str, tuple[gym.Space, gym.Space]]:
        return {
            INPUT_ENV_SPACES: (self.config.observation_space, self.config.action_space),
            DEFAULT_MODULE_ID: (
                self.config.observation_space,
                self.config.action_space,
            ),
        }
