from typing import Any, Dict, Mapping
from ray.rllib.algorithms.dqn.dqn_learner import QF_PREDS, QF_TARGET_PREDS
from ray.rllib.core.learner.learner import LearnerHyperparameters
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


class DQNTfLearner(TfLearner):
    """Implements loss and update logic for DQN."""

    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # The DQN Module returns ACTIONS for the maximum
        # Q values.
        q_values_t = fwd_out[QF_PREDS]
        q_target_values_tp1 = fwd_out[QF_TARGET_PREDS]

        terminated_mask = tf.cast(batch[SampleBatch.TERMINATEDS], tf.float32)

        # Create a mask for non-terminal experiences.
        q_target_values_tp1 = (1.0 - terminated_mask) * q_target_values_tp1
        # Compute right-hand side of Bellman equation to compute training targets.
        # TODO (simon): Make it n-step. This means also that the `TargetEncoder`
        # has to account for this. Maybe another solution is better here.
        q_t_targets = batch[SampleBatch.REWARDS] + hps.gamma * q_target_values_tp1
        # Compute TD error.
        # TODO (simon): Check, if `stop_gradient()` is even needed when
        # `trainable` is set to `False` in the `DQNRLModule`.
        td_error = q_values_t - tf.stop_gradient(q_t_targets)
        # TODO (simon): If recurrent network is used we must use a mask.
        total_loss = tf.reduce_mean(
            tf.cast(batch[SampleBatch.PRIO_WEIGHTS], tf.float32)
            * hps.td_error_loss_fn(td_error)
        )

        self.register_metrics(
            {
                "total_loss": total_loss,
                "mean_q": tf.reduce_mean(q_values_t),
                "min_q": tf.reduce_min(q_values_t),
                "max_q": tf.reduce_max(q_values_t),
                "mean_td_error": tf.reduce_mean(td_error),
            }
        )

        return total_loss

    @override(TfLearner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        timestep: int,
        **kwargs
    ) -> Dict[str, Any]:
        # Only update each `target_network_update_freq`.
        if hps.target_network_update_freq >= self._metrics["num_grad_updates"]:
            # `self.module` is a MARLModule.
            module = self.module[module_id]

            # Update weights from encoder and head with weigths from
            # q networks.
            target_and_q_network_pairs = module.get_target_network_pairs()
            for target_network, q_network in target_and_q_network_pairs:
                target_network.set_weights(q_network.get_weights())
