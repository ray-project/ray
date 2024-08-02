from typing import Dict

from ray.rllib.cql.cql_learner import CQLLearner
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
    QF_PREDS,
    TD_ERROR_MEAN_KEY,
)
from ray.rllib.algorithms.sac.torch.sac_torch_learner import SACTorchLearner
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType

torch, nn = try_import_torch()


class CQLTorchLearner(CQLLearner, SACTorchLearner):
    @override(SACTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: CQLConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        # Get the train action distribution for the current policy and current state.
        # This is needed for the policy (actor) loss and the `alpha`` loss.
        action_dist_class = self.module[module_id].get_train_action_dist_cls()
        action_dist_curr = action_dist_class.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        # Sample actions for the current state. Note that we need to apply the
        # reparameterization trick here to avoid the expectation over actions.
        actions_curr = (
            action_dist_curr.rsample()
            if not config._deterministic_loss
            # If deterministic, we use the mean.s
            else action_dist_curr.to_deterministic().sample()
        )
        # Compute the log probabilities for the current state (for the alpha loss)
        logps_curr = action_dist_curr.logp(actions_curr)

        # Optimize also the hyperparameter `alpha` by using the current policy
        # evaluated at the current state (from offline data). Note, in contrast
        # to the original SAC loss, here the `alpha` and actor losses are
        # calculated first.
        # TODO (simon): Check, why log(alpha) is used, prob. just better
        # to optimize and monotonic function. Original equation uses alpha.
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id]
            * (logps_curr.detach() + self.target_entropy[module_id])
        )

        # Get the current batch size. Note, this size might vary in case the
        # last batch contains less than `train_batch_size_per_learner` examples.
        batch_size = batch[Columns.OBS].shape[0]
        # Optimize the hyperparameter `alpha` by using the current policy evaluated
        # at the current state. Note further, we minimize here, while the original
        # equation in Haarnoja et al. (2018) considers maximization.
        if batch_size == config.train_batch_size_per_learner:
            optim = self.get_optimizer(module_id=module_id, optimizer_name="alpha")
            optim.zero_grad(set_to_none=True)
            alpha_loss.backward()
            # Add the gradients to the gradient buffer that is evaluated later in
            # `self.apply_gradients`.
            self.grads.update(
                {
                    pid: p.grad.clone()
                    for pid, p in self.filter_param_dict_for_optimizer(
                        self._params, optim
                    ).items()
                }
            )

        # Get the current alpha.
        alpha = torch.exp(self.curr_log_alpha[module_id])
        if self.metrics.peek("current_iteration") >= config.bc_iterations:
            q_selected = fwd_out[QF_PREDS]
            # TODO (simon): Add twin Q
            actor_loss = torch.mean(alpha.detach() * logps_curr - q_selected)
        else:
            bc_logps_curr = action_dist_curr.logp(batch[Columns.ACTIONS])
            actor_loss = torch.mean(alpha.detach() * logps_curr - bc_logps_curr)

        # Optimize the SAC actor loss.
        if batch_size == config.train_batch_size_per_learner:
            optim = self.get_optimizer(module_id=module_id, optimizer_name="policy")
            optim.zero_grad()
            actor_loss.backward()
            # Add the gradients to the gradient buffer that is used
            # in `self.apply_gradients`.
            self.grads.update(
                {
                    pid: p.grad.clone()
                    for pid, p in self.filter_param_dict_for_optimizer(
                        self._params, optim
                    ).items()
                }
            )

        # The critic loss is composed of the standard SAC Critic L2 loss and the
        # CQL Entropy loss.
        action_dist_next = action_dist_class.from_logits(
            fwd_out["action_dist_inputs_next"]
        )
        # Sample the actions for the next state.
        actions_next = (
            # Note, we do not need to backpropagate through the
            # next actions.
            action_dist_next.sample()
            if not config._deterministic_loss
            else action_dist_next.to_deterministic().sample()
        )
        # Compute the log probabilities for the next actions.
        logps_next = action_dist_next.logp(actions_next)

        # Get the Q-values for the actually selected actions in the offline data.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]
        # TODO (simon): Implement twin Q

        # Compute Q-values from the target Q network for the next state with the
        # sampled actions for the next state.
        q_batch_next = {
            Columns.OBS: batch[Columns.NEXT_OBS],
            Columns.ATIONS: actions_next,
        }
        q_target_next = self.module[module_id].forward_target(q_batch_next)
        # TODO (simon): Apply twin Q

        # Now mask all Q-values with terminating next states in the targets.
        q_next_masked = (1.0 - batch[Columns.TERMINATEDS].float()) * q_target_next

        # Compute the right hand side of the Bellman equation. Detach this node
        # from the computation graph as we do not want to backpropagate through
        # the target netowrk when optimizing the Q loss.
        q_selected_target = (
            # TODO (simon): Add an `n_step` option to the `AddNextObsToBatch` connector.
            batch[Columns.REWARDS]
            + (config.gamma ** batch["n_step"]) * q_next_masked
        ).detach()

        # Calculate the TD error.
        td_error = torch.abs(q_selected - q_selected_target)
        # TODO (simon): Add the Twin TD error

        # MSBE loss for the critic(s) (i.e. Q, see eqs. (7-8) Haarnoja et al. (2018)).
        # Note, this needs a sample from the current policy given the next state.
        # Note further, we could also use here the Huber loss instead of the MSE.
        # TODO (simon): Add the huber loss as an alternative (SAC uses it).
        sac_critic_loss = torch.nn.MSELoss(reduction="mean")(
            q_selected, q_selected_target
        )
        # TODO (simon): Add the Twin Q critic loss

        # Now calculate the CQL loss (we use the entropy version of the CQL algorithm).
        # Note, the entropy version performs best in shown experiments.
        actions_rand_repeat = torch.FloatTensor(
            batch[Columns.ACTIONS].shape[0] * config.num_actions,
            device=fwd_out[QF_PREDS].device,
        ).uniform_(
            self.module.config.action_space.low, self.module.config.action_space.high
        )
        # TODO (simon): Check, if we can repeat these like this or if we need to
        # backpropagate through these actions.
        # actions_curr_repeat = actions_curr[actions_curr.
        # multinomial(config.num_actions, replacement=True).view(-1)]
        # actions_next_repeat = actions_curr[actions_next.
        # multinomial(config.num_actions, replacement=True).view(-1)]
        actions_curr_repeat = (
            action_dist_curr.rsample()
            if not config._deterministic_loss
            else action_dist_curr.to_deterministic().sample()
        )
        logps_curr_repeat = action_dist_curr.logp(actions_curr_repeat)
        random_idx = actions_curr_repeat.multinomial(
            config.num_actions, replacement=True
        ).view(-1)
        actions_curr_repeat = actions_curr_repeat[random_idx]
        logps_curr_repeat = logps_curr_repeat[random_idx]
        q_batch_curr_repeat = {
            Columns.OBS: batch[Columns.OBS][random_idx],
            Columns.ACTIONS: actions_curr_repeat,
        }
        q_curr_repeat = self.module.compute_q_values(q_batch_curr_repeat)
        del q_batch_curr_repeat
        # Sample actions for the next state.
        actions_next_repeat = (
            action_dist_next.rsample()
            if not config._deterministic_loss
            else action_dist_next.to_deterministic().sample()
        )
        logps_next_repeat = action_dist_next.logp(actions_next_repeat)
        random_idx = actions_next_repeat.multinomial(
            config.num_actions, replacement=True
        ).view(-1)
        actions_next_repeat = actions_next_repeat[random_idx]
        logps_next_repeat = logps_next_repeat[random_idx]
        q_batch_next_repeat = {
            Columns.OBS: batch[Columns.NEXT_OBS][random_idx],
            Columns.ACTIONS: actions_next_repeat,
        }
        q_next_repeat = self.module.compute_q_values(q_batch_next_repeat)
        del q_batch_next_repeat

        q_batch_random_repeat = {
            # Note, we can use here simply the same random index
            # as within the last batch.
            Columns.OBS: batch[Columns.OBS][random_idx],
            Columns.ACTIONS: actions_rand_repeat,
        }
        # Compute the Q-values for the random actions (from the mu-distribution).
        q_random_repeat = self.module.compute_q_values(q_batch_random_repeat)
        del q_batch_random_repeat
        # TODO (simon): Check, if this should be `actions_random_repeat`.
        logps_random_repeat = torch.logp(0.5**actions_curr_repeat)
        q_repeat = torch.cat(
            [
                q_random_repeat - logps_random_repeat,
                q_curr_repeat - logps_curr_repeat,
                q_next_repeat - logps_next_repeat,
            ],
            dim=1,
        )
        # TODO (simon): Also run the twin Q here.

        # Compute the entropy version of the CQL loss (see eq. (4) in Kumar et al.
        # (2020)). Note that we use here the softmax with a temperature parameter.
        cql_loss = (
            torch.logsumexp(q_repeat / config.temperature, dim=1).mean()
            * config.min_q_weight
            * config.temperature
        )
        # The actual minimum Q-loss subtracts the value V (i.e. the expected Q-value)
        # evaluated at the actually selected actions.
        cql_loss = cql_loss - q_selected.mean() * config.min_q_weight
        # TODO (simon): Implement CQL twin-Q loss here

        # TODO (simon): Check, if we need to implement here also a Lagrangian
        # loss.

        critic_loss = sac_critic_loss + cql_loss
        # TODO (simon): Add here also the critic loss for the twin-Q

        # If the batch size is large enough optimize the critic.
        if batch_size == config.train_batch_size_per_learner:
            critic_optim = self.get_optimizer(module_id=module_id, optimizer_name="qf")
            critic_optim.zero_grad(set_to_none=True)
            critic_loss.backward(retain_graph=True)
            # Add the gradients to the gradient buffer that is evaluated
            # in `self.apply_gradients` later.
            self.grads.update(
                {
                    pid: p.grad.clone()
                    for pid, p in self.filter_param_dict_for_optimizer(
                        self._params, optim
                    ).items()
                }
            )
            # TODO (simon): Also optimize the twin-Q.

        total_loss = actor_loss + critic_loss + alpha_loss
        # TODO (simon): Add Twin Q losses

        # Log important loss stats (reduce=mean (default), but with window=1
        # in order to keep them history free).
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
                # TODO (simon): Add these keys to SAC Learner.
                "cql_loss": cql_loss,
                "alpha_loss": alpha_loss,
                "alpha_value": alpha,
                "log_alpha_value": torch.log(alpha),
                "target_entropy": self.target_entropy[module_id],
                "actions_curr_policy": torch.mean(actions_curr),
                LOGPS_KEY: torch.mean(logps_curr),
                QF_MEAN_KEY: torch.mean(q_curr_repeat),
                QF_MAX_KEY: torch.max(q_curr_repeat),
                QF_MIN_KEY: torch.min(q_curr_repeat),
                TD_ERROR_MEAN_KEY: torch.mean(td_error),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # TODO (simon): Add loss keys for langrangian, if needed.
        # TODO (simon): Add only here then the Langrange parameter optimization.
        # TODO (simon): Add keys for twin Q

        # Return the total loss.
        return total_loss

    @override(SACTorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[str, TensorType], **kwargs
    ) -> ParamDict:

        # Return here simply the buffered gradients from `compute_loss_for_module`.
        grads = self.grads
        # Reset the gradient buffer.
        self.grads = {}
        # Finally, return the gradients.
        return grads
