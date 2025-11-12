from typing import Dict

from ray.rllib.algorithms.dqn.dqn_learner import DQNLearner
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
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
        # Build the `DQNLearner` (builds the target network).
        super().build()

        # Define the expectile parameter(s).
        self.expectile: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                # Note, we want to train with a certain expectile.
                [self.config.get_config_for_module(module_id).expectile],
                trainable=False,
            )
        )

        # Define the temperature for the actor advantage loss.
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

    @override(DQNLearner)
    def remove_module(self, module_id: ModuleID) -> None:
        """Removes the expectile and temperature for removed modules."""
        # First call `super`'s `remove_module` method.
        super().remove_module(module_id)
        # Remove the expectile from the mapping.
        self.expectile.pop(module_id, None)
        # Remove the temperature from the mapping.
        self.temperature.pop(module_id, None)

    @override(DQNLearner)
    def add_module(
        self,
        *,
        module_id,
        module_spec,
        config_overrides=None,
        new_should_module_be_updated=None
    ):
        """Adds the expectile and temperature for new modules."""
        # First call `super`'s `add_module` method.
        super().add_module(
            module_id=module_id,
            module_spec=module_spec,
            config_overrides=config_overrides,
            new_should_module_be_updated=new_should_module_be_updated,
        )
        # Add the expectile to the mapping.
        self.expectile[module_id] = self._get_tensor_variable(
            # Note, we want to train with a certain expectile.
            [self.config.get_config_for_module(module_id).beta],
            trainable=False,
        )
        # Add the temperature to the mapping.
        self.temperature[module_id] = self._get_tensor_variable(
            # Note, we want to train with a certain expectile.
            [self.config.get_config_for_module(module_id).beta],
            trainable=False,
        )
