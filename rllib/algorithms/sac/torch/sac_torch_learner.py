from typing import Mapping
from ray.rllib.algorithms.dqn.dqn_tf_policy import PRIO_WEIGHTS
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.sac_learner import QF_PREDS, SACLearner
from ray.rllib.core.learner.learner import (
    LOGPS_KEY,
    POLICY_LOSS_KEY,
    QF_LOSS_KEY,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType


torch, nn = try_import_torch()


class SACTorchLearner(SACLearner, TorchLearner):
    """Implements `torch`-specific SAC loss logic on top of `SACLearner`

    This ' Learner' class implements the loss in its
    `self.compute_loss_for_module()` method.
    """

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: SACConfig,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # Only for debugging.
        deterministic = self.config["_deterministic_loss"]

        # Receive the current alpha hyperparameter.
        alpha = torch.exp(self.curr_log_alpha[module_id].get_current_value())

        # Get the train action distribution for the current policy and current state.
        # This is needed for the policy (actor) loss in SAC.
        action_dist_class_curr = self.module[module_id].get_train_action_dist_cls()
        action_dist_curr = action_dist_class_curr.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        # Get the train action distribution for the current policy and next state.
        # For the Q (critic) loss in SAC, we need to sample from the current policy at
        # the next state.
        action_dist_next = action_dist_class_curr.from_logits(
            fwd_out["action_dist_inputs_next"]
        )

        # Sample actions for the current state.
        actions_curr = (
            action_dist_curr.sample()
            if not deterministic
            # If deterministic, we use the mean.
            else action_dist_curr.to_deterministic.sample()
        )
        # Compute the log probabilities for the current state (for the critic loss).
        logps_curr = torch.unsqueeze(action_dist_curr.logp(actions_curr), dim=-1)

        # Sample actions for the next state.
        actions_next = (
            action_dist_next.sample()
            if not deterministic
            # If deterministic, we use the mean.
            else action_dist_next.to_deterministic().sample()
        )
        # Compute the log probabilities for the next state.
        logps_next = torch.unsqueeze(action_dist_next.logp(actions_next), dim=-1)

        # Get Q-values for the actually selected actions during rollout.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]

        # TODO (simon): Implement twin Q.

        # Compute Q-values for the current policy in the current state with
        # the sampled actions.
        q_batch_curr = NestedDict(
            {
                # TODO (simon): Check, if we need to convert to tensor here.
                SampleBatch.OBS: batch[SampleBatch.OBS],
                SampleBatch.ACTIONS: actions_curr,
            }
        )
        q_curr = self.module[module_id]._qf_forward_train(q_batch_curr)

        # Compute Q-values from the target Q network for the next state with the
        # sampled actions for the next state.
        q_batch_next = NestedDict(
            {
                # TODO (simon): Check, if we need to convert to tensor here.
                SampleBatch.OBS: batch[SampleBatch.OBS_NEXT],
                SampleBatch.ACTIONS: actions_next,
            }
        )
        q_next = self.module[module_id]._qf_forward_train(q_batch_next)
        # Compute value function for next state (see eq. (3) in Haarnoja et al. (2018)).
        # NOte, we use here the sampled actions in the log probabilities.
        q_next -= alpha * logps_next
        # Now mask all Q-values with terminate next states in the targets.
        q_next_masked = (1.0 - batch[SampleBatch.TERMINATEDS].float()) * q_next

        # Compute the right hand side of the Bellman equation.
        # Detach this node from the computation graph as we do not want to
        # backpropagate through the target network when optimizing the Q loss.
        q_selected_target = (
            batch[SampleBatch.REWARDS]
            + (self.config["gamma"] ** self.config["n_step"]) * q_next_masked
        ).detach()

        # MSBE loss for the critic(s) (i.e. Q, see eqs. (7-8) Haarnoja et al. (2018)).
        # Note, this needs a sample from the current policy given the next state.
        # Note further, we use here the Huber loss instead of the mean squared error
        # as it improves training performance.
        critic_loss = torch.mean(
            batch[PRIO_WEIGHTS]
            * torch.nn.HuberLoss(reduction=None, delta=1.0)(
                q_selected, q_selected_target
            )
        )

        # For the actor (policy) loss we need sampled actions from the current policy
        # evaluated at the current state.
        # Note further, we minimize here, while the original equation in Haarnoja et
        # al. (2018) considers maximization.
        actor_loss = torch.mean(alpha.detach() * logps_curr - q_curr)

        # Optimize also the hyperparameter alpha by using the current policy
        # evaluated at the current state (sampled values).
        # TODO (simon): Check, if this is indeed log(alpha), prob. just better
        # to optimize and monotonic function.
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id].get_current_value()
            * (logps_curr.detach() + self.config["target_entropy"])
        )

        total_loss = actor_loss + critic_loss + alpha_loss

        self.register_metrics(
            module_id,
            {
                POLICY_LOSS_KEY: torch.mean(actor_loss),
                QF_LOSS_KEY: torch.mean(critic_loss),
                "alpha_loss": torch.mean(alpha_loss),
                "alpha_value": alpha,
                # TODO (Sven): Do we really need this? We have alpha.
                "log_alpha_value": torch.log(alpha),
                "target_entropy": self.config["target_entropy"],
                "actions_curr_policy": torch.mean(action_dist_curr),
                LOGPS_KEY: torch.mean(logps_curr),
                QF_MEAN_KEY: torch.mean(q_curr),
                QF_MAX_KEY: torch.max(q_curr),
                QF_MIN_KEY: torch.min(q_curr),
            },
        )

        return total_loss

    @override(SACLearner)
    def _update_module_target_networks(
        self, module_id: ModuleID, config: SACConfig
    ) -> None:
        """Updates the target Q network(s) of a module.

        Applies Polyak averaging for the update.
        """
        module = self.module[module_id]

        # Note, we have pairs of encoder and head networks.
        target_current_network_pairs = module.get_target_network_pairs()
        for target_network, current_network in target_current_network_pairs:
            # Get the current parameters from the Q network.
            current_state_dict = current_network.state_dict()
            # Use here Polyak avereging.
            new_state_dict = {
                k: config.tau * current_state_dict[k] + (1 - config.tau) * v
                for k, v in target_network.state_dict().items()
            }
            # Apply the new parameters to the target Q network.
            target_network.load_state_dict(new_state_dict)
