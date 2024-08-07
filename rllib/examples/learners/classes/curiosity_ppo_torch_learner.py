from typing import Any, Dict, Tuple

from ray.rllib.algorithms.algorithm_config import NotProvided
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.core import Columns, DEFAULT_MODULE_ID
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.rl_modules.classes.inverse_dynamics_model_rlm import (
    InverseDynamicsModel,
)
from ray.rllib.utils.metrics import ALL_MODULES

ICM_MODULE_ID = "_inverse_dynamics_model"


class PPOConfigWithCuriosity(PPOConfig):
    # Define defaults.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.curiosity_feature_net_hiddens = (256, 256)
        self.curiosity_feature_net_activation = "relu"
        self.curiosity_inverse_net_hiddens = (256, 256)
        self.curiosity_inverse_net_activation = "relu"
        self.curiosity_forward_net_hiddens = (256, 256)
        self.curiosity_forward_net_activation = "relu"
        self.curiosity_beta = 0.2
        self.curiosity_eta = 1.0

    # Allow users to change curiosity settings.
    def curiosity(
        self,
        *,
        curiosity_feature_net_hiddens: Tuple[int, ...] = NotProvided,
        curiosity_feature_net_activation: str = NotProvided,
        curiosity_inverse_net_hiddens: Tuple[int, ...] = NotProvided,
        curiosity_inverse_net_activation: str = NotProvided,
        curiosity_forward_net_hiddens: Tuple[int, ...] = NotProvided,
        curiosity_forward_net_activation: str = NotProvided,
        curiosity_beta: float = NotProvided,
        curiosity_eta: float = NotProvided,
    ):
        if curiosity_feature_net_hiddens is not NotProvided:
            self.curiosity_feature_net_hiddens = curiosity_feature_net_hiddens
        if curiosity_feature_net_activation is not NotProvided:
            self.curiosity_feature_net_activation = curiosity_feature_net_activation
        if curiosity_inverse_net_hiddens is not NotProvided:
            self.curiosity_inverse_net_hiddens = curiosity_inverse_net_hiddens
        if curiosity_inverse_net_activation is not NotProvided:
            self.curiosity_inverse_net_activation = curiosity_inverse_net_activation
        if curiosity_forward_net_hiddens is not NotProvided:
            self.curiosity_forward_net_hiddens = curiosity_forward_net_hiddens
        if curiosity_forward_net_activation is not NotProvided:
            self.curiosity_forward_net_activation = curiosity_forward_net_activation
        if curiosity_beta is not NotProvided:
            self.curiosity_beta = curiosity_beta
        if curiosity_eta is not NotProvided:
            self.curiosity_eta = curiosity_eta
        return self


class PPOTorchLearnerWithCuriosity(PPOTorchLearner):
    def build(self):
        super().build()

        # Assert, we are only training one policy (RLModule).
        assert len(self.module) == 1 and DEFAULT_MODULE_ID in self.module

        # Add an InverseDynamicsModel to our MARLModule.
        icm_spec = RLModuleSpec(
            module_class=InverseDynamicsModel,
            observation_space=self.module[DEFAULT_MODULE_ID].config.observation_space,
            action_space=self.module[DEFAULT_MODULE_ID].config.action_space,
            model_config_dict={
                "feature_net_hiddens": self.config.curiosity_feature_net_hiddens,
                "feature_net_activation": self.config.curiosity_feature_net_activation,
                "inverse_net_hiddens": self.config.curiosity_inverse_net_hiddens,
                "inverse_net_activation": self.config.curiosity_inverse_net_activation,
                "forward_net_hiddens": self.config.curiosity_forward_net_hiddens,
                "forward_net_activation": self.config.curiosity_forward_net_activation,
            },
        )
        self.add_module(
            module_id=ICM_MODULE_ID,
            module_spec=icm_spec,
        )

        # Prepend a "add-NEXT_OBS-from-episodes-to-train-batch" connector piece (right
        # after the corresponding "add-OBS-..." default piece).
        if self.config.add_default_connectors_to_learner_pipeline:
            self._learner_connector.insert_after(
                AddObservationsFromEpisodesToBatch,
                AddNextObservationsFromEpisodesToTrainBatch(),
            )

    def compute_loss(
        self,
        *,
        fwd_out: Dict[str, Any],
        batch: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Compute the ICM loss first (so we'll have the chance to change the rewards
        # in the batch for the "main" RLModule (before we compute its loss with the
        # intrinsic rewards).
        icm = self.module[ICM_MODULE_ID]
        # Send the exact same batch to the ICM module that we used for the "main"
        # RLModule's forward pass.
        icm_fwd_out = icm.forward_train(batch=batch[DEFAULT_MODULE_ID])
        # Compute the loss of the ICM module.
        icm_loss = icm.compute_loss_for_module(
            learner=self,
            module_id=ICM_MODULE_ID,
            config=self.config.get_config_for_module(ICM_MODULE_ID),
            batch=batch[DEFAULT_MODULE_ID],
            fwd_out=icm_fwd_out,
        )

        # Add intrinsic rewards from ICM's `fwd_out` (multiplied by factor `eta`)
        # to "main" module batch's extrinsic rewards.
        batch[DEFAULT_MODULE_ID][Columns.REWARDS] += (
            self.config.curiosity_eta * icm_fwd_out[Columns.INTRINSIC_REWARDS]
        )

        # Compute the "main" RLModule's loss.
        main_loss = self.compute_loss_for_module(
            module_id=DEFAULT_MODULE_ID,
            config=self.config.get_config_for_module(DEFAULT_MODULE_ID),
            batch=batch[DEFAULT_MODULE_ID],
            fwd_out=fwd_out[DEFAULT_MODULE_ID],
        )

        return {
            DEFAULT_MODULE_ID: main_loss,
            ICM_MODULE_ID: icm_loss,
            ALL_MODULES: main_loss + icm_loss,
        }
