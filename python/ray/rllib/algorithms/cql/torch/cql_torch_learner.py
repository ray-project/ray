from typing import Dict

from ray.rllib.algorithms.cql.cql import CQLConfig
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MAX_KEY,
    QF_MEAN_KEY,
    QF_MIN_KEY,
    QF_PREDS,
    QF_TWIN_LOSS_KEY,
    QF_TWIN_PREDS,
    TD_ERROR_MEAN_KEY,
)
from ray.rllib.algorithms.sac.torch.sac_torch_learner import SACTorchLearner
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType
from ray.tune.result import TRAINING_ITERATION

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

        # Optimize also the hyperparameter `alpha` by using the current policy
        # evaluated at the current state (from offline data). Note, in contrast
        # to the original SAC loss, here the `alpha` and actor losses are
        # calculated first.
        # TODO (simon): Check, why log(alpha) is used, prob. just better
        # to optimize and monotonic function. Original equation uses alpha.
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id]
            * (fwd_out["logp_resampled"].detach() + self.target_entropy[module_id])
        )

        # Get the current alpha.
        alpha = torch.exp(self.curr_log_alpha[module_id])
        # Start training with behavior cloning and turn to the classic Soft-Actor Critic
        # after `bc_iters` of training iterations.
        if (
            self.metrics.peek((ALL_MODULES, TRAINING_ITERATION), default=0)
            >= config.bc_iters
        ):
            actor_loss = torch.mean(
                alpha.detach() * fwd_out["logp_resampled"] - fwd_out["q_curr"]
            )
        else:
            # Use log-probabilities of the current action distribution to clone
            # the behavior policy (selected actions in data) in the first `bc_iters`
            # training iterations.
            bc_logps_curr = action_dist_curr.logp(batch[Columns.ACTIONS])
            actor_loss = torch.mean(
                alpha.detach() * fwd_out["logp_resampled"] - bc_logps_curr
            )

        # The critic loss is composed of the standard SAC Critic L2 loss and the
        # CQL entropy loss.

        # Get the Q-values for the actually selected actions in the offline data.
        # In the critic loss we use these as predictions.
        q_selected = fwd_out[QF_PREDS]
        if config.twin_q:
            q_twin_selected = fwd_out[QF_TWIN_PREDS]

        if not config.deterministic_backup:
            q_next = (
                fwd_out["q_target_next"]
                - alpha.detach() * fwd_out["logp_next_resampled"]
            )
        else:
            q_next = fwd_out["q_target_next"]

        # Now mask all Q-values with terminating next states in the targets.
        q_next_masked = (1.0 - batch[Columns.TERMINATEDS].float()) * q_next

        # Compute the right hand side of the Bellman equation. Detach this node
        # from the computation graph as we do not want to backpropagate through
        # the target network when optimizing the Q loss.
        # TODO (simon, sven): Kumar et al. (2020) use here also a reward scaler.
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

        # Compute the log-probabilities for the random actions (note, we generate random
        # actions (from the mu distribution as named in Kumar et al. (2020))).
        # Note, all actions, action log-probabilities and Q-values are already computed
        # by the module's `_forward_train` method.
        # TODO (simon): This is the density for a discrete uniform, however, actions
        # come from a continuous one. So actually this density should use (1/(high-low))
        # instead of (1/2).
        random_density = torch.log(
            torch.pow(
                0.5,
                torch.tensor(
                    fwd_out["actions_curr_repeat"].shape[-1],
                    device=fwd_out["actions_curr_repeat"].device,
                ),
            )
        )
        # Merge all Q-values and subtract the log-probabilities (note, we use the
        # entropy version of CQL).
        q_repeat = torch.cat(
            [
                fwd_out["q_rand_repeat"] - random_density,
                fwd_out["q_next_repeat"] - fwd_out["logps_next_repeat"].detach(),
                fwd_out["q_curr_repeat"] - fwd_out["logps_curr_repeat"].detach(),
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
                    fwd_out["q_twin_rand_repeat"] - random_density,
                    fwd_out["q_twin_next_repeat"]
                    - fwd_out["logps_next_repeat"].detach(),
                    fwd_out["q_twin_curr_repeat"]
                    - fwd_out["logps_curr_repeat"].detach(),
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
                "alpha_value": alpha[0],
                "log_alpha_value": torch.log(alpha)[0],
                "target_entropy": self.target_entropy[module_id],
                LOGPS_KEY: torch.mean(
                    fwd_out["logp_resampled"]
                ),  # torch.mean(logps_curr),
                QF_MEAN_KEY: torch.mean(fwd_out["q_curr_repeat"]),
                QF_MAX_KEY: torch.max(fwd_out["q_curr_repeat"]),
                QF_MIN_KEY: torch.min(fwd_out["q_curr_repeat"]),
                TD_ERROR_MEAN_KEY: torch.mean(td_error),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        self._temp_losses[(module_id, POLICY_LOSS_KEY)] = actor_loss
        self._temp_losses[(module_id, QF_LOSS_KEY)] = critic_loss
        self._temp_losses[(module_id, "alpha_loss")] = alpha_loss

        # TODO (simon): Add loss keys for langrangian, if needed.
        # TODO (simon): Add only here then the Langrange parameter optimization.
        if config.twin_q:
            self.metrics.log_value(
                key=(module_id, QF_TWIN_LOSS_KEY),
                value=critic_twin_loss,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )
            self._temp_losses[(module_id, QF_TWIN_LOSS_KEY)] = critic_twin_loss

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
                loss_tensor = self._temp_losses.pop((module_id, optim_name + "_loss"))
                loss_tensor.backward(
                    retain_graph=False if optim_name in ["policy", "alpha"] else True
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

        assert not self._temp_losses
        return grads
