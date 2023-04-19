"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Any, Dict, Mapping

from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
    VF_LOSS_KEY,
    ENTROPY_KEY,
)
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


class DreamerV3TfLearner(TfLearner):
    """Implements DreamerV3 losses and update logic in TensorFlow.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]
        old_target_policy_dist = fwd_out[OLD_ACTION_DIST_KEY]


        return {
            self.TOTAL_LOSS_KEY: total_loss,
            POLICY_LOSS_KEY: mean_pi_loss,
            VF_LOSS_KEY: mean_vf_loss,
            ENTROPY_KEY: mean_entropy_loss,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
            LEARNER_RESULTS_CURR_KL_COEFF_KEY: self.kl_coeffs[module_id],
        }

    @override(TfLearner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kls: Dict[ModuleID, float], **kwargs
    ) -> Mapping[str, Any]:
        """Update the target networks and KL loss coefficients of each module.

        Args:

        """
        #self._update_module_target_networks(module_id)
        #self._update_module_kl_coeff(module_id, sampled_kls)
        #return {}
