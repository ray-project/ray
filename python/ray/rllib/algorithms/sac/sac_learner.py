from typing import Dict

import numpy as np

from ray.rllib.algorithms.dqn.dqn_learner import DQNLearner
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.typing import ModuleID, TensorType

# Now, this is double defined: In `DefaultSACRLModule` and here. I would keep it here
# or push it into the `Learner` as these are recurring keys in RL.
LOGPS_KEY = "logps"
QF_LOSS_KEY = "qf_loss"
QF_MEAN_KEY = "qf_mean"
QF_MAX_KEY = "qf_max"
QF_MIN_KEY = "qf_min"
QF_PREDS = "qf_preds"
QF_TWIN_LOSS_KEY = "qf_twin_loss"
QF_TWIN_PREDS = "qf_twin_preds"
TD_ERROR_MEAN_KEY = "td_error_mean"
CRITIC_TARGET = "critic_target"
ACTION_DIST_INPUTS_NEXT = "action_dist_inputs_next"
QF_TARGET_NEXT = "q_target_next"
ACTION_PROBS_NEXT = "action_probs_next"
ACTION_LOG_PROBS_NEXT = "action_log_probs_next"
ACTION_PROBS = "action_probs"
ACTION_LOG_PROBS = "action_log_probs"


class SACLearner(DQNLearner):
    @override(Learner)
    def build(self) -> None:
        # Store the current alpha in log form. We need it during optimization
        # in log form.
        self.curr_log_alpha: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                # Note, we want to train the temperature parameter.
                [
                    np.log(
                        self.config.get_config_for_module(module_id).initial_alpha
                    ).astype(np.float32)
                ],
                trainable=True,
            )
        )

        # We need to call the `super()`'s `build()` method here to have the variables
        # for the alpha already defined.
        super().build()

        self.target_entropy: Dict[ModuleID, TensorType] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                self._get_target_entropy(module_id)
            )
        )

    @override(Learner)
    def remove_module(self, module_id: ModuleID) -> None:
        """Removes the temperature and target entropy.

        Note, this means that we also need to remove the corresponding
        temperature optimizer.
        """
        super().remove_module(module_id)
        self.curr_log_alpha.pop(module_id, None)
        self.target_entropy.pop(module_id, None)

    @override(Learner)
    def add_module(
        self,
        *,
        module_id,
        module_spec,
        config_overrides=None,
        new_should_module_be_updated=None
    ):
        # First call `super`'s `add_module` method.
        super().add_module(
            module_id=module_id,
            module_spec=module_spec,
            config_overrides=config_overrides,
            new_should_module_be_updated=new_should_module_be_updated,
        )
        # Now add the log alpha.
        self.curr_log_alpha[module_id] = self._get_tensor_variable(
            # Note, we want to train the temperature parameter.
            [
                np.log(
                    self.config.get_config_for_module(module_id).initial_alpha
                ).astype(np.float32)
            ],
            trainable=True,
        )
        # Add also the target entropy for the new module.
        self.target_entropy[module_id] = self._get_tensor_variable(
            self._get_target_entropy(module_id)
        )

    def _get_target_entropy(self, module_id):
        """Returns the target entropy to use for the loss.

        Args:
            module_id: Module ID for which the target entropy should be
                returned.

        Returns:
            Target entropy.
        """
        target_entropy = self.config.get_config_for_module(module_id).target_entropy
        if target_entropy is None or target_entropy == "auto":
            target_entropy = -np.prod(
                self._module_spec.module_specs[module_id].action_space.shape
            )
        return target_entropy
