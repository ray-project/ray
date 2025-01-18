import logging
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.learner import GeneralAdvantageEstimation
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.annotations import override
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (
    AddNextObservationsFromEpisodesToTrainBatch,
)

"""
Custom PPO Config class that inherits PPOConfig and overrides the base class's AlgorithmConfig build_learner_connector
method to construct a custom pipeline that adds on the ability to have 'new_obs' in the training batch
"""


class CustomPPOConfig(PPOConfig):
    @override(AlgorithmConfig)
    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):

        pipeline = super().build_learner_connector(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            device=device,
        )
        # insert the new_obs to the training batch
        pipeline.insert_after(
            name_or_class=AddObservationsFromEpisodesToBatch,
            connector=AddNextObservationsFromEpisodesToTrainBatch(),
        )
        # make sure advantages are also added to the batch
        pipeline.append(
            GeneralAdvantageEstimation(gamma=self.gamma, lambda_=self.lambda_)
        )

        logging.info(
            "Inserted AddNextObservationsFromEpisodesToTrainBatch and GeneralAdvantageEstimation into the learner pipeline."
        )

        return pipeline
