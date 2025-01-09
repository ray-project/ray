import logging
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import AddNextObservationsFromEpisodesToTrainBatch
# from ray.rllib.connectors.learner import AddObservationsFromEpisodesToBatch
from ray.rllib.connectors.learner import (
    AddOneTsToEpisodesAndTruncate,
    GeneralAdvantageEstimation,
)
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.annotations import override
from ray.rllib.algorithms.ppo.ppo import PPO


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

        print("**************(((((((((((using custom config)))))))))))")

        pipeline.prepend(AddOneTsToEpisodesAndTruncate(input_observation_space, input_action_space))
        
        pipeline.insert_after(
            AddNextObservationsFromEpisodesToTrainBatch(input_observation_space, input_action_space)
        )
        pipeline.append(
            GeneralAdvantageEstimation(
                gamma=self.gamma, lambda_=self.lambda_
            )
        )

        logging.info(
            "Inserted AddNextObservationsFromEpisodesToTrainBatch and GeneralAdvantageEstimation into the learner pipeline."
        )

        return pipeline
