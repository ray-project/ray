from typing import Any, Dict

from ray.rllib.algorithms.algorithm_config import NotProvided
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.core import Columns, DEFAULT_MODULE_ID
from ray.rllib.utils.metrics import ALL_MODULES

ICM_MODULE_ID = "_intrinsic_curiosity_model"


def make_curiosity_config_class(config_class):
    class _class(config_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            # Define defaults.
            self.curiosity_beta = 0.2
            self.curiosity_eta = 1.0

        # Allow users to change curiosity settings.
        def curiosity(
            self,
            *,
            curiosity_beta: float = NotProvided,
            curiosity_eta: float = NotProvided,
        ):
            """Sets the config's curiosity settings.

            Args:
                curiosity_beta: The coefficient used for the intrinsic rewards. Overall
                    rewards are computed as `R = R[extrinsic] + beta * R[intrinsic]`.
                curiosity_eta: Fraction of the forward loss (within the total loss term)
                    vs the inverse dynamics loss. The total loss of the ICM is computed
                    as: `L = eta * [forward loss] + (1.0 - eta) * [inverse loss]`.

            Returns:
                This updated AlgorithmConfig object.
            """
            if curiosity_beta is not NotProvided:
                self.curiosity_beta = curiosity_beta
            if curiosity_eta is not NotProvided:
                self.curiosity_eta = curiosity_eta
            return self

    return _class


def make_curiosity_learner_class(learner_class):
    class _class(learner_class):
        def build(self):
            super().build()

            # Assert, we are only training one policy (RLModule) and we have the ICM
            # in our MultiRLModule.
            assert (
                len(self.module) == 2
                and DEFAULT_MODULE_ID in self.module
                and ICM_MODULE_ID in self.module
            )

            # Prepend a "add-NEXT_OBS-from-episodes-to-train-batch" connector piece
            # (right after the corresponding "add-OBS-..." default piece).
            if self.config.add_default_connectors_to_learner_pipeline:
                self._learner_connector.insert_after(
                    AddObservationsFromEpisodesToBatch,
                    AddNextObservationsFromEpisodesToTrainBatch(),
                )

        def compute_losses(
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
            # Log the env steps trained counter for the ICM

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

    return _class
