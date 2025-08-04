from typing import Dict

from ray.rllib.algorithms.dqn.dqn_learner import DQNLearner
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.typing import ModuleID, TensorType

QF_TARGET_PREDS = "qf_target_preds"
VF_PREDS_NEXT = "vf_preds_next"
VF_LOSS = "value_loss"


class IQLLearner(DQNLearner):

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(DQNLearner)
    def build(self) -> None:
        # Define the expectile parameter.
        self.expectile: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                # Note, we want to train with a certain expectile.
                [self.config.get_config_for_module(module_id).expectile],
                trainable=False,
            )
        )

        # Define the temperature for the acotr advantage loss.
        self.temperature: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                # Note, we want to train with a certain expectile.
                [self.config.get_config_for_module(module_id).beta],
                trainable=False,
            )
        )

        # Store loss tensors here temporarily inside the loss function for (exact)
        # consumption later by the compute gradients function.
        # Keys=(module_id, optimizer_name), values=loss tensors (in-graph).
        self._temp_losses = {}

        # Build the DQNLearner (builds the target network).
        super().build()

    @override(DQNLearner)
    def remove_module(self, module_id: ModuleID) -> None:
        """Removes the expectile."""
        super().remove_module(module_id)
        self.expectile.pop(module_id, None)
