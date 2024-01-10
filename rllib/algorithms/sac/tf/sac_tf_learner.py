from typing import Mapping
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.sac_learner import QF_PREDS, QF_TARGET_PREDS
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
        action_dist = action_dist_class_train.from_logits(fwd_out[SampleBatch.ACTION_DIST_INPUTS])

        # Get the action distribution for the next time step.
        # TODO (simon): Add "action_dist_inputs_next" as a constant.
        action_dist_next = action_dist_class_train.from_logits(fwd_out["action_dist_inputs_next"])

        # If deterministic, convert action distributions to deterministic ones, i.e.
        # distributions that choose a maximizing action.
        if deterministic
            action_dist = action_dist.to_deterministic()
            action_dist_next = action_dist_next.to_deterministic()

        # Sample actions.
        actions = action_dist.sample()
        logpis = action_dist.logp(actions)
        actions_next = action_dist_next.sample()
        logpis_next = action_dist_next.logp(actions_next)

        # Q-values for the actually selected actions.
        q_vals = fwd_out[QF_PREDS]

        # Q-values for the current policy in given current state.

        q_vals_curr_policy = self.module[module_id].forward_train()
 
