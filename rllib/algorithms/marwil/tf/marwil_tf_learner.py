import logging

from typing import Mapping

from ray.rllib.algorithms.marwil.marwil_learner import (
    LEARNER_RESULTS_MOVING_AVG_SQD_ADV_NORM_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VARIANCE_KEY,
    MARWILLearner,
    MARWILLearnerHyperparameters,
)
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()
logger = logging.getLogger(__file__)


class MARWILTfLearner(MARWILLearner, TfLearner):
    """Implements tf-specific MARWIL loss logic on top of Learner.

    This class implements the MARWIL loss under `self.compute_loss_for_module()`.
    """

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: MARWILLearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType],
    ) -> TensorType:
        # RNN case: Mask away 0-padded chunks at the end of the time axis.
        if self.module[module_id].is_stateful():
            # In case of RNN, we expect the incoming tensors to be padded to the
            # maximum sequence length `max_seq_len`. We infer the maximum sequence
            # from the actions tensor.
            maxlen = tf.math.reduce_max(batch[SampleBatch.ACTIONS])
            mask = tf.sequence_mask(batch[SampleBatch.SEQ_LENS], maxlen)

            def possibly_masked_mean(t):
                return tf.reduce_mean(tf.boolean_mask(t, mask))

        else:
            # non-RNN case: no masking.
            mask = None
            possibly_masked_mean = tf.reduce_mean

        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        curr_action_dist = action_dist_class_train(
            batch[SampleBatch.ACTION_DIST_INPUTS]
        )
        logprobs = curr_action_dist.logp(batch[SampleBatch.ACTIONS])
        value_fn_out = fwd_out[SampleBatch.VF_PREDS]

        if hps.beta != 0.0:
            # Note that we do not use GAE when calling `compute_advantages()`,
            # i.e. `SampleBatch.ADVANTAGES` do contain the cumulative rewards
            # and not the advantages.
            # TODO (simon): Check, if using GAE give better results.
            cumulative_rewards = batch[Postprocessing.ADVANTAGES]
            # Advantages estimation.
            advantages = cumulative_rewards - value_fn_out
            # TODO (simon): Check, if masked mean is needed here.
            advantages_sqd = possibly_masked_mean(tf.math.square(advantages))
            # Value function loss.
            # TODO (sven): Why scaled by 0.5?
            vf_loss = 0.5 * advantages_sqd

            # Update averaged advantages norm.
            update_term = (
                advantages_sqd - self.moving_avg_sqd_adv_norms_per_module[module_id]
            )
            # TODO (simon): Check performance, if this goes into the
            # `additional_update_for_module()` and is updated AFTER loss computation
            # for the next iteration.
            self.moving_avg_sqd_adv_norms_per_module.assign_add(
                hps.moving_average_sqd_adv_norm_update_rate * update_term
            )

            # Exponentially weighted advantages.
            c = tf.math.sqrt(self.moving_avg_sqd_adv_norms_per_module[module_id])
            advantages_exp = tf.math.exp(hps.beta * (advantages / (1e-8 + c)))
            advantages_exp = tf.stop_gradient(advantages_exp)

        else:
            # Value function's loss term.
            vf_loss = tf.constant(0.0)
            advantages_exp = 1.0

        if hps.bc_logstd_coeff > 0.0:
            log_stds = tf.reduce_sum(curr_action_dist.log_std, axis=1)
        else:
            log_stds = 0.0

        policy_loss = possibly_masked_mean(
            advantages_exp * (logprobs + hps.bc_logstd_coeff * log_stds)
        )

        total_loss = -policy_loss + hps.vf_coeff * vf_loss

        self.register_metrics(
            module_id,
            {
                POLICY_LOSS_KEY: -policy_loss,
                LEARNER_RESULTS_MOVING_AVG_SQD_ADV_NORM_KEY: (
                    self.moving_avg_sqd_adv_norms_per_module[module_id]
                )
                if hps.beta != 0.0
                else None,
                LEARNER_RESULTS_VF_EXPLAINED_VARIANCE_KEY: explained_variance(
                    cumulative_rewards, value_fn_out
                )
                if hps.beta != 0.0
                else None,
                VF_LOSS_KEY: vf_loss if hps.beta != 0.0 else None,
            },
        )

        return total_loss
