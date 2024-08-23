import tree
from typing import Dict

from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
    QF_PREDS,
    QF_TWIN_LOSS_KEY,
    QF_TWIN_PREDS,
    TD_ERROR_MEAN_KEY,
)
from ray.rllib.algorithms.cql.cql import CQLConfig
from ray.rllib.algorithms.sac.torch.sac_torch_learner import SACTorchLearner
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType

torch, nn = try_import_torch()


class CQLTorchLearner(SACTorchLearner):
    @override(SACTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: CQLConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        # TODO (simon, sven): Add upstream information pieces into this timesteps
        #  call arg to Learner.update_...().
        self.metrics.log_value(
            (ALL_MODULES, TRAINING_ITERATION),
            1,
            reduce="sum",
        )
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

        # Get the current alpha.
        alpha = torch.exp(self.curr_log_alpha[module_id])
        # Start training with behavior cloning and turn to the classic Soft-Actor Critic
        # after `bc_iters` of training iterations.
        if (
            self.metrics.peek((ALL_MODULES, TRAINING_ITERATION), default=0)
            >= config.bc_iters
        ):
            # Calculate current Q-values.
            batch_curr = {
                Columns.OBS: batch[Columns.OBS],
                # Use the actions sampled from the current policy.
                Columns.ACTIONS: actions_curr,
            }
            # Note, if `twin_q` is `True`, `compute_q_values` computes the minimum
            # of the `qf` and `qf_twin` and returns this minimum.
            q_curr = self.module[module_id].compute_q_values(batch_curr)
            actor_loss = torch.mean(alpha.detach() * logps_curr - q_curr)
        else:
            # Use log-probabilities of the current action distribution to clone
            # the behavior policy (selected actions in data) in the first `bc_iters`
            # training iterations.
            bc_logps_curr = action_dist_curr.logp(batch[Columns.ACTIONS])
            actor_loss = torch.mean(alpha.detach() * logps_curr - bc_logps_curr)

        # The critic loss is composed of the standard SAC Critic L2 loss and the
        # CQL entropy loss.
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

        # Get the Q-values for the actually selected actions in the offline data.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]
        if config.twin_q:
            q_twin_selected = fwd_out[QF_TWIN_PREDS]

        # Compute Q-values from the target Q network for the next state with the
        # sampled actions for the next state.
        q_batch_next = {
            Columns.OBS: batch[Columns.NEXT_OBS],
            Columns.ACTIONS: actions_next,
        }
        # Note, if `twin_q` is `True`, `SACTorchRLModule.forward_target` calculates
        # the Q-values for both, `qf_target` and `qf_twin_target` and
        # returns the minimum.
        q_target_next = self.module[module_id].forward_target(q_batch_next)

        # Now mask all Q-values with terminating next states in the targets.
        q_next_masked = (1.0 - batch[Columns.TERMINATEDS].float()) * q_target_next

        # Compute the right hand side of the Bellman equation. Detach this node
        # from the computation graph as we do not want to backpropagate through
        # the target netowrk when optimizing the Q loss.
        q_selected_target = (
            # TODO (simon): Add an `n_step` option to the `AddNextObsToBatch` connector.
            batch[Columns.REWARDS]
            # TODO (simon): Implement n_step.
            + (config.gamma) * q_next_masked
        ).detach()

        # Calculate the TD error.
        td_error = torch.abs(q_selected - q_selected_target)
        # Calculate a TD-error for twin-Q values, if needed.
        if config.twin_q:
            td_error += torch.abs(q_twin_selected - q_selected_target)
            # Rescale the TD error
            td_error *= 0.5

        # MSBE loss for the critic(s) (i.e. Q, see eqs. (7-8) Haarnoja et al. (2018)).
        # Note, this needs a sample from the current policy given the next state.
        # Note further, we could also use here the Huber loss instead of the MSE.
        # TODO (simon): Add the huber loss as an alternative (SAC uses it).
        sac_critic_loss = torch.nn.MSELoss(reduction="mean")(
            q_selected,
            q_selected_target,
        )
        if config.twin_q:
            sac_critic_twin_loss = torch.nn.MSELoss(reduction="mean")(
                q_twin_selected,
                q_selected_target,
            )

        # Now calculate the CQL loss (we use the entropy version of the CQL algorithm).
        # Note, the entropy version performs best in shown experiments.
        # Generate random actions (from the mu distribution as named in Kumar et
        # al. (2020))
        low = torch.tensor(
            self.module[module_id].config.action_space.low,
            device=fwd_out[QF_PREDS].device,
        )
        high = torch.tensor(
            self.module[module_id].config.action_space.high,
            device=fwd_out[QF_PREDS].device,
        )
        num_samples = batch[Columns.ACTIONS].shape[0] * config.num_actions
        actions_rand_repeat = low + (high - low) * torch.rand(
            (num_samples, low.shape[0]), device=fwd_out[QF_PREDS].device
        )

        # Sample current and next actions (from the pi distribution as named in Kumar
        # et al. (2020)) using repeated observations.
        actions_curr_repeat, logps_curr_repeat, obs_curr_repeat = self._repeat_actions(
            action_dist_class, batch[Columns.OBS], config.num_actions, module_id
        )
        actions_next_repeat, logps_next_repeat, obs_next_repeat = self._repeat_actions(
            action_dist_class, batch[Columns.NEXT_OBS], config.num_actions, module_id
        )

        # Calculate the Q-values for all actions.
        batch_rand_repeat = {
            Columns.OBS: obs_curr_repeat,
            Columns.ACTIONS: actions_rand_repeat,
        }
        # Note, we need here the Q-values from the base Q-value function
        # and not the minimum with an eventual Q-value twin.
        q_rand_repeat = (
            self.module[module_id]
            ._qf_forward_train_helper(
                batch_rand_repeat,
                self.module[module_id].qf_encoder,
                self.module[module_id].qf,
            )
            .view(batch_size, config.num_actions, 1)
        )
        # Calculate twin Q-values for the random actions, if needed.
        if config.twin_q:
            q_twin_rand_repeat = (
                self.module[module_id]
                ._qf_forward_train_helper(
                    batch_rand_repeat,
                    self.module[module_id].qf_twin_encoder,
                    self.module[module_id].qf_twin,
                )
                .view(batch_size, config.num_actions, 1)
            )
        del batch_rand_repeat
        batch_curr_repeat = {
            Columns.OBS: obs_curr_repeat,
            Columns.ACTIONS: actions_curr_repeat,
        }
        q_curr_repeat = (
            self.module[module_id]
            ._qf_forward_train_helper(
                batch_curr_repeat,
                self.module[module_id].qf_encoder,
                self.module[module_id].qf,
            )
            .view(batch_size, config.num_actions, 1)
        )
        # Calculate twin Q-values for the repeated actions from the current policy,
        # if needed.
        if config.twin_q:
            q_twin_curr_repeat = (
                self.module[module_id]
                ._qf_forward_train_helper(
                    batch_curr_repeat,
                    self.module[module_id].qf_twin_encoder,
                    self.module[module_id].qf_twin,
                )
                .view(batch_size, config.num_actions, 1)
            )
        del batch_curr_repeat
        batch_next_repeat = {
            # Note, we use here the current observations b/c we want to keep the
            # state fix while sampling the actions.
            Columns.OBS: obs_curr_repeat,
            Columns.ACTIONS: actions_next_repeat,
        }
        q_next_repeat = (
            self.module[module_id]
            ._qf_forward_train_helper(
                batch_next_repeat,
                self.module[module_id].qf_encoder,
                self.module[module_id].qf,
            )
            .view(batch_size, config.num_actions, 1)
        )
        # Calculate also the twin Q-values for the current policy and next actions,
        # if needed.
        if config.twin_q:
            q_twin_next_repeat = (
                self.module[module_id]
                ._qf_forward_train_helper(
                    batch_next_repeat,
                    self.module[module_id].qf_twin_encoder,
                    self.module[module_id].qf_twin,
                )
                .view(batch_size, config.num_actions, 1)
            )
        del batch_next_repeat

        # Compute the log-probabilities for the random actions.
        # TODO (simon): This is the density for a discrete uniform, however, actions
        # come from a continuous one. So actually this density should use (1/(high-low))
        # instead of (1/2).
        random_density = torch.log(
            torch.pow(
                torch.tensor(
                    actions_curr_repeat.shape[-1], device=actions_curr_repeat.device
                ),
                0.5,
            )
        )
        # Merge all Q-values and subtract the log-probabilities (note, we use the
        # entropy version of CQL).
        q_repeat = torch.cat(
            [
                q_rand_repeat - random_density,
                q_next_repeat - logps_next_repeat.detach(),
                q_curr_repeat - logps_curr_repeat.detach(),
            ],
            dim=1,
        )
        cql_loss = (
            torch.logsumexp(q_repeat / config.temperature, dim=1).mean()
            * config.min_q_weight
            * config.temperature
        )
        cql_loss -= q_selected.mean() * config.min_q_weight
        # Add the CQL loss term to the SAC loss term.
        critic_loss = sac_critic_loss + cql_loss

        # If a twin Q-value function is implemented calculated its CQL loss.
        if config.twin_q:
            q_twin_repeat = torch.cat(
                [
                    q_twin_rand_repeat - random_density,
                    q_twin_next_repeat - logps_next_repeat.detach(),
                    q_twin_curr_repeat - logps_curr_repeat.detach(),
                ],
                dim=1,
            )
            cql_twin_loss = (
                torch.logsumexp(q_twin_repeat / config.temperature, dim=1).mean()
                * config.min_q_weight
                * config.temperature
            )
            cql_twin_loss -= q_twin_selected.mean() * config.min_q_weight
            # Add the CQL loss term to the SAC loss term.
            critic_twin_loss = sac_critic_twin_loss + cql_twin_loss

        # TODO (simon): Check, if we need to implement here also a Lagrangian
        # loss.

        total_loss = actor_loss + critic_loss + alpha_loss

        # Add the twin critic loss to the total loss, if needed.
        if config.twin_q:
            # Reweigh the critic loss terms in the total loss.
            total_loss += 0.5 * critic_twin_loss - 0.5 * critic_loss

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
        if config.twin_q:
            self.metrics.log_dict(
                {
                    QF_TWIN_LOSS_KEY: critic_twin_loss,
                },
                key=module_id,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )

        # Return the total loss.
        return total_loss

    @override(SACTorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
    ) -> ParamDict:

        grads = {}
        for module_id in set(loss_per_module.keys()) - {ALL_MODULES}:
            # Loop through optimizers registered for this module.
            for optim_name, optim in self.get_optimizers_for_module(module_id):
                # Zero the gradients. Note, we need to reset the gradients b/c
                # each component for a module operates on the same graph.
                optim.zero_grad(set_to_none=True)

                # Compute the gradients for the component and module.
                self.metrics.peek((module_id, optim_name + "_loss")).backward(
                    retain_graph=True
                )
                # Store the gradients for the component and module.
                # TODO (simon): Check another time the graph for overlapping
                # gradients.
                grads.update(
                    {
                        pid: grads[pid] + p.grad.clone()
                        if pid in grads
                        else p.grad.clone()
                        for pid, p in self.filter_param_dict_for_optimizer(
                            self._params, optim
                        ).items()
                    }
                )

        return grads

    def _repeat_tensor(self, tensor, repeat):
        """Generates a repeated version of a tensor.

        The repetition is done similar `np.repeat` and repeats each value
        instead of the complete vector.

        Args:
            tensor: The tensor to be repeated.
            repeat: How often each value in the tensor should be repeated.

        Returns:
            A tensor holding `repeat`  repeated values of the input `tensor`
        """
        # Insert the new dimension at axis 1 into the tensor.
        t_repeat = tensor.unsqueeze(1)
        # Repeat the tensor along the new dimension.
        t_repeat = torch.repeat_interleave(t_repeat, repeat, dim=1)
        # Stack the repeated values into the batch dimension.
        t_repeat = t_repeat.view(-1, *tensor.shape[1:])
        # Return the repeated tensor.
        return t_repeat

    def _repeat_actions(self, action_dist_class, obs, num_actions, module_id):
        """Generated actions for repeated observations.

        The `num_actions` define a multiplier used for generating `num_actions`
        as many actions as the batch size. Observations are repeated and then a
        model forward pass is made.

        Args:
            action_dist_class: The action distribution class to be sued for sampling
                actions.
            obs: A batched observation tensor.
            num_actions: The multiplier for actions, i.e. how much more actions
                than the batch size should be generated.
            module_id: The module ID to be used when calling the forward pass.

        Returns:
            A tuple containing the sampled actions, their log-probabilities and the
            repeated observations.
        """
        # Receive the batch size.
        batch_size = obs.shape[0]
        # Repeat the observations `num_actions` times.
        obs_repeat = tree.map_structure(
            lambda t: self._repeat_tensor(t, num_actions), obs
        )
        # Generate a batch for the forward pass.
        temp_batch = {Columns.OBS: obs_repeat}
        # Run the forward pass in inference mode.
        fwd_out = self.module[module_id].forward_inference(temp_batch)
        # Generate the squashed Gaussian from the model's logits.
        action_dist = action_dist_class.from_logits(fwd_out[Columns.ACTION_DIST_INPUTS])
        # Sample the actions. Note, we want to make a backward pass through
        # these actions.
        actions = action_dist.rsample()
        # Compute the action log-probabilities.
        action_logps = action_dist.logp(actions).view(batch_size, num_actions, 1)

        # Return
        return actions, action_logps, obs_repeat
