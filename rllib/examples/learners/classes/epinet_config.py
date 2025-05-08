import logging
from typing import Any, Dict, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.learners.classes.epinet_learner import (
    PPOTorchLearnerWithEpinetLoss,
)
from ray.rllib.examples.learners.classes.epinet_rlm import EpinetTorchRLModule
from ray.rllib.utils.annotations import override

from ray.rllib.utils.typing import EnvType
import gymnasium as gym


class PPOConfigWithEpinet(PPOConfig):
    """
    A custom PPOConfig that specifies a custom Epinet critic network RLModule, a Learner class,
    and adds on to the builder pipeline.

    The `get_rl_module_spec` allows for flexibility with specifying the custom RL module that is used.
    In this case, it is EpinetTorchRLModule.

    The `get_default_learner_class` simply imports the new `PPOTorchLearnerWithEpinetLoss` class which
    inherits from `PPOTorchLearner` and uses the custom loss associated the epinet.

    To have the 'NEXT_OBS' 'batch' we need to add the `AddNextObservationsFromEpisodesToTrainBatch`
    class to the pipeline and insert after adding the `AddObservationsFromEpisodesToBatch` to the
    batch.

    The `_model_config_auto_includes` method includes custom parameters that we wish to pass through
    the `_model_config`. In this case, the params "z_dim", "num_layers", and "enn_layer_size" are passed
    from args to the RL module for the epinet specifically. While "fcnet_hiddens" and "fcnet_activation"
    can also be passed to the RL module for building the actor-critic networks.
    """

    def __init__(self):
        # Args can be passed from epinet.py to the PPOConfigWithEpinet to set model configuration
        # or to have access in the model_config of epinet_rlm.py class (EpinetTorchRLModule)
        # We are setting the base parameters of the epinet here for instantiation.
        super().__init__()

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Learner:
        # Override the learner with the custom learner class
        # We can also use the below as a lazy import:
        # return from ray.rllib.examples.learners.classes.epinet_learner import (
        #     PPOTorchLearnerWithEpinetLoss,
        # )
        return PPOTorchLearnerWithEpinetLoss

    @override(AlgorithmConfig)
    def get_rl_module_spec(
        self,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[str, gym.Space]] = None,
        inference_only: Optional[bool] = None,
    ) -> RLModuleSpec:
        spec = super().get_rl_module_spec(env, spaces, inference_only)
        spec.module_class = EpinetTorchRLModule
        return spec

    @override(PPOConfig)
    def training(self, *, enn_network, z_dim, **kwargs) -> "PPOConfigWithEpinet":
        # Call `super`'s `training` method for PPO's parameters and unpack them.
        super().training(**kwargs)
        # Add custom parameters to have access during the training loop
        if enn_network is not NotProvided:
            self.enn_network = enn_network
        if z_dim is not NotProvided:
            self.z_dim = z_dim
        return self

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
            "Inserted `AddNextObservationsFromEpisodesToTrainBatch` into the learner pipeline."
        )

        return pipeline

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        # Alters the self.model_config dict to contain the components for the epinet.
        return super()._model_config_auto_includes | {
            "initializer": "xavier_normal_",
            "z_dim": self.z_dim,
            "enn_network": self.enn_network,
            "fcnet_hiddens": [128, 128],
            "fcnet_activation": "LeakyReLU",
        }
