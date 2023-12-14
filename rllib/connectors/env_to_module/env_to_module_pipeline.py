from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.connectors.env_to_module.default_env_to_module import DefaultEnvToModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class EnvToModulePipeline(ConnectorPipelineV2):
    def __init__(
        self,
        *,
        connectors: Optional[List[ConnectorV2]] = None,
        input_observation_space: Optional[gym.Space],
        input_action_space: Optional[gym.Space],
        env: Optional[gym.Env] = None,
        rl_module: Optional["RLModule"] = None,
        **kwargs,
    ):
        super().__init__(
            connectors=connectors,
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            env=env,
            rl_module=rl_module,
            **kwargs,
        )
        # Add the default final connector piece for env-to-module pipelines:
        # Extracting last obs from episodes and add them to input, iff this has not
        # happened in any connector piece in this pipeline before.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultEnvToModule
        ):
            self.append(DefaultEnvToModule(
                input_observation_space=self.observation_space,
                input_action_space=self.action_space,
                env=env,
            ))

    @override(ConnectorPipelineV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        input_: Optional[Any] = None,
        episodes: List[EpisodeType],
        explore: bool,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ):
        # Make sure user does not necessarily send initial input into this pipeline.
        # Might just be empty and to be populated from `episodes`.
        return super().__call__(
            rl_module=rl_module,
            input_=input_ if input_ is not None else {},
            episodes=episodes,
            explore=explore,
            persistent_data=persistent_data,
            **kwargs,
        )
