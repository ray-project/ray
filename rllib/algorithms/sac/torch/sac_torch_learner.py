from typing import Dict, Mapping
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

# from ray.rllib.algorithms.dqn.dqn_tf_policy import PRIO_WEIGHTS
from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
    TD_ERROR_KEY,
    QF_PREDS,
    QF_TWIN_LOSS_KEY,
    QF_TWIN_PREDS,
    SACLearner,
)
from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType


torch, nn = try_import_torch()


class SACTorchLearner(SACLearner, TorchLearner):
    """Implements `torch`-specific SAC loss logic on top of `SACLearner`

    This ' Learner' class implements the loss in its
    `self.compute_loss_for_module()` method.
    """

    # TODO (simon): Set different learning rates for optimizers.
    @override(TorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: AlgorithmConfig = None, hps=None
    ) -> None:
        # Receive the module.
        module = self._module[module_id]

        # Define the optimizer for the critic.
        # TODO (sven): Maybe we change here naming to `qf` for unification.
        params_critic = self.get_parameters(module.qf_encoder) + self.get_parameters(
            module.qf
        )
        optim_critic = torch.optim.Adam(params_critic, eps=1e-7)

        self.register_optimizer(
            module_id=module_id,
            optimizer_name="qf",
            optimizer=optim_critic,
            params=params_critic,
            lr_or_lr_schedule=self.config.lr,
        )
        # If necessary register also an optimizer for a twin Q network.
        if self.config.twin_q:
            params_twin_critic = self.get_parameters(
                module.qf_twin_encoder
            ) + self.get_parameters(module.qf_twin)
            optim_twin_critic = torch.optim.Adam(params_twin_critic, eps=1e-7)

            self.register_optimizer(
                module_id=module_id,
                optimizer_name="qf",
                optimizer=optim_twin_critic,
                params=params_twin_critic,
                lr_or_lr_schedule=self.config.lr,
            )

        # Define the optimizer for the actor.
        params_actor = self.get_parameters(module.pi_encoder) + self.get_parameters(
            module.pi
        )
        optim_actor = torch.optim.Adam(params_actor, eps=1e-7)

        self.register_optimizer(
            module_id=module_id,
            optimizer_name="policy",
            optimizer=optim_actor,
            params=params_actor,
            lr_or_lr_schedule=self.config.lr,
        )

        # Define the optimizer for the temperature.
        temperature = self.curr_log_alpha[module_id]
        optim_temperature = torch.optim.Adam([temperature], eps=1e-7)
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="alpha",
            optimizer=optim_temperature,
            params=[temperature],
            lr_or_lr_schedule=self.config.lr,
        )

    @override(TorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[str, TensorType], **kwargs
    ) -> ParamDict:
        for optim in self._optimizer_parameters:
            optim.zero_grad(set_to_none=True)

        grads = {}

        # Calculate gradients for each loss by its optimizer.
        # TODO (sven): Maybe we rename to `actor`, `critic`. We then also
        # need to either add to or change in the `Learner` constants.
        for component in (
            ["qf", "policy", "alpha"] + ["qf_twin"] if self.config.twin_q else []
        ):
            self._metrics[DEFAULT_POLICY_ID][component + "_loss"].backward(
                retain_graph=True
            )
            grads.update(
                {
                    pid: p.grad
                    for pid, p in self.filter_param_dict_for_optimizer(
                        self._params, self.get_optimizer(optimizer_name=component)
                    ).items()
                }
            )

        return grads

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
        deterministic = self.config._deterministic_loss

        # Receive the current alpha hyperparameter.
        alpha = torch.exp(self.curr_log_alpha[module_id])

        # Get the train action distribution for the current policy and current state.
        # This is needed for the policy (actor) loss in SAC.
        action_dist_class = self.module[module_id].get_train_action_dist_cls()
        action_dist_curr = action_dist_class.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        # Get the train action distribution for the current policy and next state.
        # For the Q (critic) loss in SAC, we need to sample from the current policy at
        # the next state.
        action_dist_next = action_dist_class.from_logits(
            fwd_out["action_dist_inputs_next"]
        )

        # Sample actions for the current state. Note that we need to apply the
        # reparameterization trick here to avoid the expectation over actions.
        actions_curr = (
            action_dist_curr.rsample()
            if not deterministic
            # If deterministic, we use the mean.
            else action_dist_curr.to_deterministic().sample()
        )
        # Compute the log probabilities for the current state (for the critic loss).
        logps_curr = action_dist_curr.logp(actions_curr)

        # Sample actions for the next state.
        actions_next = (
            action_dist_next.sample()
            if not deterministic
            # If deterministic, we use the mean.
            else action_dist_next.to_deterministic().sample()
        )
        # Compute the log probabilities for the next state.
        logps_next = action_dist_next.logp(actions_next)

        # Get Q-values for the actually selected actions during rollout.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]
        if self.config.twin_q:
            q_twin_selected = fwd_out[QF_TWIN_PREDS]

        # TODO (simon): Implement twin Q.

        # Compute Q-values for the current policy in the current state with
        # the sampled actions.
        q_batch_curr = NestedDict(
            {
                SampleBatch.OBS: batch[SampleBatch.OBS],
                SampleBatch.ACTIONS: actions_curr,
            }
        )
        q_curr = self.module[module_id]._qf_forward_train(q_batch_curr)[QF_PREDS]
        # If a twin Q network should be used, calculate twin Q-values and use the
        # minimum.
        if self.config.twin_q:
            q_twin_curr = self.module[module_id]._qf_twin_forward_train(q_batch_curr)[
                QF_PREDS
            ]
            q_curr = torch.min(q_curr, q_twin_curr, dim=-1)

        # Compute Q-values from the target Q network for the next state with the
        # sampled actions for the next state.
        q_batch_next = NestedDict(
            {
                SampleBatch.OBS: batch[SampleBatch.NEXT_OBS],
                SampleBatch.ACTIONS: actions_next,
            }
        )
        q_target_next = self.module[module_id]._qf_target_forward_train(q_batch_next)[
            QF_PREDS
        ]
        # If a twin Q network should be used, calculate twin Q-values and use the
        # minimum.
        if self.config.twin_q:
            q_target_twin_next = self.module[module_id]._qf_target_twin_forward_train(
                q_batch_next
            )
            q_target_next = torch.min(q_target_next, q_target_twin_next, dim=-1)

        # Compute value function for next state (see eq. (3) in Haarnoja et al. (2018)).
        # Note, we use here the sampled actions in the log probabilities.
        q_target_next -= alpha * logps_next
        # Now mask all Q-values with terminated next states in the targets.
        q_next_masked = (
            (1.0 - batch[SampleBatch.TERMINATEDS].float())
            # If the current experience is the last in the episode we neglect it as
            # otherwise the next value would be from a new episode's reset observation.
            # See for more information the `EpisodeReplayBuffer.sample()` with
            # `batch_length_T > 0`.
            * (1.0 - batch["is_last"].float())
            * q_target_next
        )

        # Compute the right hand side of the Bellman equation.
        # Detach this node from the computation graph as we do not want to
        # backpropagate through the target network when optimizing the Q loss.
        q_selected_target = (
            batch[SampleBatch.REWARDS]
            # TODO (simon): Implement n-step adjustment.
            # + (self.config["gamma"] ** self.config["n_step"]) * q_next_masked
            + self.config.gamma * q_next_masked
        ).detach()

        # Calculate the TD-error. Note, this is needed for the priority weights in
        # the replay buffer.
        td_error = torch.abs(q_selected - q_selected_target)
        # If a twin Q network should be used, add the TD error of the twin Q network.
        if self.config.twin_q:
            td_error += torch.abs(q_twin_selected, q_selected_target)
            # Rescale the TD error.
            td_error *= 0.5

        # MSBE loss for the critic(s) (i.e. Q, see eqs. (7-8) Haarnoja et al. (2018)).
        # Note, this needs a sample from the current policy given the next state.
        # Note further, we use here the Huber loss instead of the mean squared error
        # as it improves training performance.
        critic_loss = torch.mean(
            # TODO (simon): Introduce priority weights when episode buffer is ready.
            # batch[PRIO_WEIGHTS] *
            torch.nn.HuberLoss(reduction="none", delta=1.0)(
                q_selected, q_selected_target
            )
        )
        # If a twin Q network should be used, add the critic loss of the twin Q network.
        if self.config.twin_q:
            critic_twin_loss = torch.mean(
                # TODO (simon): Introduce priority weights when episode buffer is ready.
                # batch[PRIO_WEIGHTS] *
                torch.nn.HuberLoss(reduction="none", delta=1.0)(
                    q_twin_selected, q_selected_target
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
            self.curr_log_alpha[module_id]
            * (logps_curr.detach() + self.target_entropy[module_id])
        )

        total_loss = actor_loss + critic_loss + alpha_loss
        # If twin Q networks should be used, add the critic loss of the twin Q network.
        if self.config.twin_q:
            total_loss += critic_twin_loss

        self.register_metrics(
            module_id,
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
                TD_ERROR_KEY: td_error,
                "alpha_loss": alpha_loss,
                "alpha_value": alpha,
                # TODO (Sven): Do we really need this? We have alpha.
                "log_alpha_value": torch.log(alpha),
                "target_entropy": self.target_entropy[module_id],
                "actions_curr_policy": torch.mean(actions_curr),
                LOGPS_KEY: torch.mean(logps_curr),
                QF_MEAN_KEY: torch.mean(q_curr),
                QF_MAX_KEY: torch.max(q_curr),
                QF_MIN_KEY: torch.min(q_curr),
            },
        )
        # If twin Q networks should be used add a critic loss for the twin Q network.
        # Note, we need this in the `self.compute_gradients()` to optimize.
        if self.config.twin_q:
            self.register_metrics(
                module_id,
                {
                    QF_TWIN_LOSS_KEY: critic_twin_loss,
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
