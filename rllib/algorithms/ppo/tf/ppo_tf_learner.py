import logging
from typing import Any, Mapping

from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType


_, tf, _ = try_import_tf()
logger = logging.getLogger(__name__)


class PPOTfLearner(PPOLearner, TfLearner):
    """Implements tf-specific PPO loss logic on top of PPOLearner.

    This class implements the ppo loss under `_compute_loss_per_module()`.
    """

    @override(TfLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # TODO (Kourosh): batch type is NestedDict.
        # TODO (Kourosh): We may or may not user module_id. For example if we have an
        # agent based learning rate scheduler, we may want to use module_id to get the
        # learning rate for that agent.
        # TODO (Kourosh): come back to RNNs later

        curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
        action_dist_class = type(fwd_out[SampleBatch.ACTION_DIST])
        prev_action_dist = action_dist_class.from_logits(
            batch[SampleBatch.ACTION_DIST_INPUTS]
        )

        logp_ratio = tf.exp(
            fwd_out[SampleBatch.ACTION_LOGP] - batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.hps.kl_coeff > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = tf.reduce_mean(action_kl)
            if tf.math.is_inf(mean_kl_loss):
                logger.warning(
                    "KL divergence is non-finite, this will likely destabilize "
                    "your model and the training process. Action(s) in a "
                    "specific state have near-zero probability. "
                    "This can happen naturally in deterministic "
                    "environments where the optimal policy has zero mass "
                    "for a specific action. To fix this issue, consider "
                    "setting the coefficient for the KL loss term to "
                    "zero or increasing policy entropy."
                )
        else:
            mean_kl_loss = tf.constant(0.0, dtype=logp_ratio.dtype)

        curr_entropy = fwd_out["entropy"]
        mean_entropy = tf.reduce_mean(curr_entropy)

        surrogate_loss = tf.minimum(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * tf.clip_by_value(
                logp_ratio, 1 - self.hps.clip_param, 1 + self.hps.clip_param
            ),
        )

        # Compute a value function loss.
        if self.hps.use_critic:
            value_fn_out = fwd_out[SampleBatch.VF_PREDS]
            vf_loss = tf.math.square(value_fn_out - batch[Postprocessing.VALUE_TARGETS])
            vf_loss_clipped = tf.clip_by_value(vf_loss, 0, self.hps.vf_clip_param)
            mean_vf_loss = tf.reduce_mean(vf_loss_clipped)
            mean_vf_unclipped_loss = tf.reduce_mean(vf_loss)
        # Ignore the value function.
        else:
            value_fn_out = tf.constant(0.0, dtype=surrogate_loss.dtype)
            mean_vf_unclipped_loss = tf.constant(0.0, dtype=surrogate_loss.dtype)
            vf_loss_clipped = mean_vf_loss = tf.constant(
                0.0, dtype=surrogate_loss.dtype
            )

        total_loss = tf.reduce_mean(
            -surrogate_loss
            + self.hps.vf_loss_coeff * vf_loss_clipped
            - self.hps.entropy_coeff * curr_entropy
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.hps.kl_coeff > 0.0:
            total_loss += self.curr_kl_coeff * mean_kl_loss

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "policy_loss": -tf.reduce_mean(surrogate_loss),
            "vf_loss": mean_vf_loss,
            "unclipped_vf_loss": mean_vf_unclipped_loss,
            "vf_explained_var": explained_variance(
                batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            "entropy": mean_entropy,
            "kl": mean_kl_loss,
            "entropy_coeff": self.hps.entropy_coeff,
            "cur_kl_coeff": self.curr_kl_coeff,
        }

    @override(PPOLearner)
    def _get_kl_variable(self, value: float) -> Any:
        return tf.Variable(value, trainable=False, dtype=tf.float32)

    @override(PPOLearner)
    def _set_kl_coeff(self, value: float) -> None:
        self.curr_kl_coeff.assign(value)
