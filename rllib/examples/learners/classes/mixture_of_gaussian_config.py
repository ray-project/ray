import logging
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.annotations import override
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.examples.learners.classes.mixture_of_gaussian_rlm import MOGTorchRLModule
from ray.rllib.examples.learners.classes.mixture_of_gaussian_learner import (
    PPOTorchLearnerWithMOGLoss,
)
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (
    AddNextObservationsFromEpisodesToTrainBatch,
)


class PPOConfigWithMOG(PPOConfig):
    """
    A custom PPOConfig that specifies a custom mixture of gaussian rl module, a learner class,
    and adds on to the builder pipeline.

    The get_default_rl_module_spec allows flexibility with specifying rl modules parameters that
    are associated with the model (model_config) as well as custom parameters made by the users,
    in this case, is the number of gaussians to use for the mixture of gaussian module.

    The get_default_learner_class simply imports the new PPOTorchLearnerWithMOGLoss class which inherits
    from PPOTorchLearner and uses the custom loss associated mixture of gaussians.

    To have the 'new_obs' in the 'Columns' and 'batch' we need to add the
    AddNextObservationsFromEpisodesToTrainBatch class to the pipeline and insert after adding the
    AddObservationsFromEpisodesToBatch to the batch.
    """

    def __init__(self, num_mog_components=3):
        super().__init__()
        # Args can be passed from mixture_of_gaussian.py to the PPOConfigWithMOG to set model configuration
        # or to have access in the model_config of mixture_of_gaussian_rlm.py class (MOGTorchRLModule)
        self.num_mog_components = num_mog_components

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Learner:
        # Override the learner with the custom learner class
        return PPOTorchLearnerWithMOGLoss

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        # Return the custom module spec / module
        custom_config = {
            "vf_share_layers": False,
            "fcnet_hiddens": [128, 128],
            "fcnet_activation": "LeakyReLU",
            "num_mog_components": self.num_mog_components,
        }
        return RLModuleSpec(
            module_class=MOGTorchRLModule,
            model_config=custom_config,
        )

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
        # Insert the new_obs to the training batch
        pipeline.insert_after(
            name_or_class=AddObservationsFromEpisodesToBatch,
            connector=AddNextObservationsFromEpisodesToTrainBatch(),
        )

        logging.info(
            "Inserted AddNextObservationsFromEpisodesToTrainBatch and GeneralAdvantageEstimation into the learner pipeline."
        )

        return pipeline
