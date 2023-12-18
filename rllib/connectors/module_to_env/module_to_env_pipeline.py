from typing import List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.connectors.module_to_env.default_module_to_env import DefaultModuleToEnv


class ModuleToEnvPipeline(ConnectorPipelineV2):
    pass

    #def __init__(
    #    self,
    #    *,
    #    connectors: Optional[List[ConnectorV2]] = None,
    #    input_observation_space: Optional[gym.Space] = None,
    #    input_action_space: Optional[gym.Space] = None,
    #    env: Optional[gym.Env] = None,
    #    rl_module: Optional[RLModule] = None,
    #    **kwargs,
    #):
    #    super().__init__(
    #        connectors=connectors,
    #        input_observation_space=input_observation_space,
    #        input_action_space=input_action_space,
    #        env=env,
    #        rl_module=rl_module,
    #        **kwargs,
    #    )
    #
    #    # Add the default final connector piece for env-to-module pipelines:
    #    # Sampling actions from action_dist_inputs and add them to input, iff this has
    #    # not happened in any connector piece in this pipeline before.
    #    if (
    #        len(self.connectors) == 0
    #        or type(self.connectors[-1]) is not DefaultModuleToEnv
    #    ):
    #        self.append(
    #            DefaultModuleToEnv(
    #                input_observation_space=self.observation_space,
    #                input_action_space=self.action_space,
    #                env=env,
    #                rl_module=rl_module,
    #            )
    #        )
    #