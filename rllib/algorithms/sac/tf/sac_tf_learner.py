from typing import Mapping
from ray.rllib.algorithms.dqn.dqn_tf_policy import PRIO_WEIGHTS
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.sac_learner import QF_PREDS
from ray.rllib.core.learner.tf.tf_learner import TfLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


class SACTfLearner(TfLearner):
    """Implements loss and update logic for SAC."""

    # TODO (simon): Implement twin Q network.
    @override(TfLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: SACConfig,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        deterministic = config["_deterministic_loss"]

        # Get the action distribution for the current time step.
        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        action_dist = action_dist_class_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )

        # Get the action distribution for the next time step.
        # TODO (simon): Add "action_dist_inputs_next" as a constant.
        action_dist_next = action_dist_class_train.from_logits(
            fwd_out["action_dist_inputs_next"]
        )

        # If deterministic, convert action distributions to deterministic ones, i.e.
        # distributions that choose a maximizing action.
        if deterministic:
            action_dist = action_dist.to_deterministic()
            action_dist_next = action_dist_next.to_deterministic()

        # Sample actions.
        actions = action_dist.sample()
        logpis = action_dist.logp(actions)
        actions_next = action_dist_next.sample()
        logpis_next = action_dist_next.logp(actions_next)

        # Q-values for the actually selected actions.
        q_vals_selected = fwd_out[QF_PREDS]

        # Q-values for the current policy in given current state.
        # TODO (simon): Check, if we have to expand dimensions for actions to have batch.
        q_batch_current = NestedDict(
            {SampleBatch.OBS: batch[SampleBatch.OBS], SampleBatch.ACTIONS: actions}
        )
        q_vals_curr_policy = self.module[module_id]._qf_forward_train(q_batch_current)

        # Target Q-values for the current policy in given next state.
        q_target_batch_next = NestedDict(
            {
                SampleBatch.OBS: batch[SampleBatch.NEXT_OBS],
                SampleBatch.ACTIONS: actions_next,
            }
        )
        q_target_vals_next = self.module[module_id]._qf_target_forward_train(
            q_target_batch_next
        )
        # Compute the soft state value function estimate.
        q_target_vals_next -= self.hps.alpha * logpis_next
        # Use soft state value estimation only for non-terminate states.
        q_target_value_next_masked = (
            1.0 - tf.cast(batch[SampleBatch.TERMINATEDS], tf.float32)
        ) * q_target_vals_next

        # Compute RHS of Bellman equation for the Q-loss (critic(s)).
        # Note, for training the Q-network we want to backpropagate only
        # through the Q network, but not the target.
        q_vals_selected_target = tf.stop_gradient(
            batch[SampleBatch.REWARDS]
            + self.config.gamma**self.config.n_step * q_target_value_next_masked
        )

        # Calculate critic loss. Note, this uses implicitly the TD error.
        critic_loss = tf.reduce_mean(
            batch[PRIO_WEIGHTS]
            * tf.losses.Huber()(q_vals_selected, q_vals_selected_target)
        )

        alpha_loss = tf.reduce_mean(
            tf.Variable(self.hps.alpha) * tf.stop_gradient(logpis + self.hps.target_entropy)
        )

        actor_loss = tf.reduce_mean(self.hps.alpha * logpis - q_vals_curr_policy)
        
        # Calculate the total loss.
        total_loss = actor_loss + tf.math.add_n(critic_loss) + alpha_loss

        return total_loss