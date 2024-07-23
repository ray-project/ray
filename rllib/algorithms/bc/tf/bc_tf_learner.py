import logging
from typing import Dict

from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModuleID, TensorType

_, tf, _ = try_import_tf()
logger = logging.getLogger(__file__)


class BCTfLearner(TfLearner):
    """Implements tf-specific BC loss logic.

    This class implements the BC loss under `self.compute_loss_for_module()`.
    """

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: BCConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType]
    ) -> TensorType:
        # In the RNN case, we expect incoming tensors to be padded to the maximum
        # sequence length. We infer the max sequence length from the actions
        # tensor.
        # TODO (sven): Unify format requirement and handling thereof.
        # - If an episode ends in the middle of a row, insert an initial state
        #  instead of zero-padding.
        # - New field in the batch dict "is_first" (shape=(B, T)) indicates at
        #  which positions to insert these initial states.
        # This removes special reduction and only needs tf.reduce_mean().
        if self.module[module_id].is_stateful():
            maxlen = tf.math.reduce_max(batch[Columns.SEQ_LENS])
            mask = tf.sequence_mask(batch[Columns.SEQ_LENS], maxlen)

            def possibly_masked_mean(t):
                return tf.reduce_mean(tf.boolean_mask(t, mask))

        # non-RNN case: use simple mean.
        else:
            possibly_masked_mean = tf.reduce_mean

        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        action_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        log_probs = action_dist.logp(batch[Columns.ACTIONS])

        policy_loss = -possibly_masked_mean(log_probs)

        # Log important loss stats.
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: policy_loss,
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )

        # Return total loss which is for BC simply the policy loss.
        return policy_loss
