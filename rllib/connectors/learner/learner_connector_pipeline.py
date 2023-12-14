from typing import List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.connectors.learner.default_learner_connector import (
    DefaultLearnerConnector,
)


class LearnerConnectorPipeline(ConnectorPipelineV2):
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

        # Add the default final connector piece for learner pipelines:
        # Making sure that we have - at the minimum - observations and that the data
        # is time-ranked (if we have a stateful model) and properly zero-padded.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultLearnerConnector
        ):
            self.append(
                DefaultLearnerConnector(
                    input_observation_space=self.observation_space,
                    input_action_space=self.action_space,
                )
            )
