from typing import Dict

from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.dqn.dqn_learner import (
    ATOMS,
    QF_LOGITS,
    QF_LOSS_KEY,
    QF_MAX_KEY,
    QF_MEAN_KEY,
    QF_MIN_KEY,
    QF_NEXT_PREDS,
    QF_PREDS,
    QF_PROBS,
    QF_TARGET_NEXT_PREDS,
    QF_TARGET_NEXT_PROBS,
    TD_ERROR_MEAN_KEY,
    DQNLearner,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import TD_ERROR_KEY
from ray.rllib.utils.typing import ModuleID, TensorType

torch, nn = try_import_torch()


class DQNTorchLearner(DQNLearner, TorchLearner):
    """Implements `torch`-specific DQN Rainbow loss logic on top of `DQNLearner`

    This ' Learner' class implements the loss in its
    `self.compute_loss_for_module()` method.
    """

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: DQNConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType]
    ) -> TensorType:

        # Possibly apply masking to some sub loss terms and to the total loss term
        # at the end. Masking could be used for RNN-based model (zero padded `batch`)
        # and for PPO's batched value function (and bootstrap value) computations,
        # for which we add an (artificial) timestep to each episode to
        # simplify the actual computation.
        if Columns.LOSS_MASK in batch:
            mask = batch[Columns.LOSS_MASK].clone()
            # Check, if a burn-in should be used to recover from a poor state.
            if self.config.burn_in_len > 0:
                # Train only on the timesteps after the burn-in period.
                mask[:, : self.config.burn_in_len] = False
            num_valid = torch.sum(mask)

            def possibly_masked_mean(data_):
                return torch.sum(data_[mask]) / num_valid

            def possibly_masked_min(data_):
                # Prevent minimum over empty tensors, which can happened
                # when all elements in the mask are `False`.
                return (
                    torch.tensor(float("nan"))
                    if data_[mask].numel() == 0
                    else torch.min(data_[mask])
                )

            def possibly_masked_max(data_):
                # Prevent maximum over empty tensors, which can happened
                # when all elements in the mask are `False`.
                return (
                    torch.tensor(float("nan"))
                    if data_[mask].numel() == 0
                    else torch.max(data_[mask])
                )

        else:
            possibly_masked_mean = torch.mean
            possibly_masked_min = torch.min
            possibly_masked_max = torch.max

        q_curr = fwd_out[QF_PREDS]
        q_target_next = fwd_out[QF_TARGET_NEXT_PREDS]

        # Get the Q-values for the selected actions in the rollout.
        # TODO (simon, sven): Check, if we can use `gather` with a complex action
        # space - we might need the one_hot_selection. Also test performance.
        q_selected = torch.nan_to_num(
            torch.gather(
                q_curr,
                dim=-1,
                index=batch[Columns.ACTIONS]
                .view(*batch[Columns.ACTIONS].shape, 1)
                .long(),
            ),
            neginf=0.0,
        ).squeeze(dim=-1)

        # Use double Q learning.
        if config.double_q:
            # Then we evaluate the target Q-function at the best action (greedy action)
            # over the online Q-function.
            # Mark the best online Q-value of the next state.
            q_next_best_idx = (
                torch.argmax(fwd_out[QF_NEXT_PREDS], dim=-1).unsqueeze(dim=-1).long()
            )
            # Get the Q-value of the target network at maximum of the online network
            # (bootstrap action).
            q_next_best = torch.nan_to_num(
                torch.gather(q_target_next, dim=-1, index=q_next_best_idx),
                neginf=0.0,
            ).squeeze()
        else:
            # Mark the maximum Q-value(s).
            q_next_best_idx = (
                torch.argmax(q_target_next, dim=-1).unsqueeze(dim=-1).long()
            )
            # Get the maximum Q-value(s).
            q_next_best = torch.nan_to_num(
                torch.gather(q_target_next, dim=-1, index=q_next_best_idx),
                neginf=0.0,
            ).squeeze()

        # If we learn a Q-distribution.
        if config.num_atoms > 1:
            # Extract the Q-logits evaluated at the selected actions.
            # (Note, `torch.gather` should be faster than multiplication
            # with a one-hot tensor.)
            # (32, 2, 10) -> (32, 10)
            q_logits_selected = torch.gather(
                fwd_out[QF_LOGITS],
                dim=1,
                # Note, the Q-logits are of shape (B, action_space.n, num_atoms)
                # while the actions have shape (B, 1). We reshape actions to
                # (B, 1, num_atoms).
                index=batch[Columns.ACTIONS]
                .view(-1, 1, 1)
                .expand(-1, 1, config.num_atoms)
                .long(),
            ).squeeze(dim=1)
            # Get the probabilies for the maximum Q-value(s).
            q_probs_next_best = torch.gather(
                fwd_out[QF_TARGET_NEXT_PROBS],
                dim=1,
                # Change the view and then expand to get to the dimensions
                # of the probabilities (dims 0 and 2, 1 should be reduced
                # from 2 -> 1).
                index=q_next_best_idx.view(-1, 1, 1).expand(-1, 1, config.num_atoms),
            ).squeeze(dim=1)

            # For distributional Q-learning we use an entropy loss.

            # Extract the support grid for the Q distribution.
            z = fwd_out[ATOMS]
            # TODO (simon): Enable computing on GPU.
            # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)s
            r_tau = torch.clamp(
                batch[Columns.REWARDS].unsqueeze(dim=-1)
                + (
                    config.gamma ** batch["n_step"]
                    * (1.0 - batch[Columns.TERMINATEDS].float())
                ).unsqueeze(dim=-1)
                * z,
                config.v_min,
                config.v_max,
            ).squeeze(dim=1)
            # (32, 10)
            b = (r_tau - config.v_min) / (
                (config.v_max - config.v_min) / float(config.num_atoms - 1.0)
            )
            lower_bound = torch.floor(b)
            upper_bound = torch.ceil(b)

            floor_equal_ceil = ((upper_bound - lower_bound) < 0.5).float()

            # (B, num_atoms, num_atoms).
            lower_projection = nn.functional.one_hot(
                lower_bound.long(), config.num_atoms
            )
            upper_projection = nn.functional.one_hot(
                upper_bound.long(), config.num_atoms
            )
            # (32, 10)
            ml_delta = q_probs_next_best * (upper_bound - b + floor_equal_ceil)
            mu_delta = q_probs_next_best * (b - lower_bound)
            # (32, 10)
            ml_delta = torch.sum(lower_projection * ml_delta.unsqueeze(dim=-1), dim=1)
            mu_delta = torch.sum(upper_projection * mu_delta.unsqueeze(dim=-1), dim=1)
            # We do not want to propagate through the distributional targets.
            # (32, 10)
            m = (ml_delta + mu_delta).detach()

            # The Rainbow paper claims to use the KL-divergence loss. This is identical
            # to using the cross-entropy (differs only by entropy which is constant)
            # when optimizing by the gradient (the gradient is identical).
            td_error = nn.CrossEntropyLoss(reduction="none")(q_logits_selected, m)
            # Compute the weighted loss (importance sampling weights).
            total_loss = torch.mean(batch["weights"] * td_error)
        else:
            # Masked all Q-values with terminated next states in the targets.
            q_next_best_masked = (
                1.0 - batch[Columns.TERMINATEDS].float()
            ) * q_next_best

            # Compute the RHS of the Bellman equation.
            # Detach this node from the computation graph as we do not want to
            # backpropagate through the target network when optimizing the Q loss.
            q_selected_target = (
                batch[Columns.REWARDS]
                + (config.gamma ** batch["n_step"]) * q_next_best_masked
            ).detach()

            # Choose the requested loss function. Note, in case of the Huber loss
            # we fall back to the default of `delta=1.0`.
            loss_fn = nn.HuberLoss if config.td_error_loss_fn == "huber" else nn.MSELoss
            # Compute the TD error.
            td_error = torch.abs(q_selected - q_selected_target)
            # Compute the weighted loss (importance sampling weights).
            total_loss = possibly_masked_mean(
                batch["weights"]
                * loss_fn(reduction="none")(q_selected, q_selected_target)
            )

        # Log the TD-error with reduce="item_series", such that - in case we have n parallel
        # Learners - we will re-concatenate the produced TD-error tensors to yield
        # a 1:1 representation of the original batch.
        self.metrics.log_value(
            key=(module_id, TD_ERROR_KEY),
            value=td_error,
            reduce="item_series",
        )
        # Log other important loss stats (reduce=mean (default), but with window=1
        # in order to keep them history free).
        self.metrics.log_dict(
            {
                QF_LOSS_KEY: total_loss,
                QF_MEAN_KEY: possibly_masked_mean(q_selected),
                QF_MAX_KEY: possibly_masked_max(q_selected),
                QF_MIN_KEY: possibly_masked_min(q_selected),
                TD_ERROR_MEAN_KEY: possibly_masked_mean(td_error),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # If we learn a Q-value distribution log the support and average
        # probabilities.
        if config.num_atoms > 1:
            # Log important loss stats.
            self.metrics.log_dict(
                {
                    ATOMS: torch.mean(z),
                    # The absolute difference in expectation between the actions
                    # should (at least mildly) rise.
                    "expectations_abs_diff": torch.mean(
                        torch.abs(
                            torch.diff(
                                torch.sum(fwd_out[QF_PROBS].mean(dim=0) * z, dim=1)
                            ).mean(dim=0)
                        )
                    ),
                    # The total variation distance should measure the distance between
                    # return distributions of different actions. This should (at least
                    # mildly) increase during training when the agent differentiates
                    # more between actions.
                    "dist_total_variation_dist": torch.diff(
                        fwd_out[QF_PROBS].mean(dim=0), dim=0
                    )
                    .abs()
                    .sum()
                    * 0.5,
                    # The maximum distance between the action distributions. This metric
                    # should increase over the course of training.
                    "dist_max_abs_distance": torch.max(
                        torch.diff(fwd_out[QF_PROBS].mean(dim=0), dim=0).abs()
                    ),
                    # Mean shannon entropy of action distributions. This should decrease
                    # over the course of training.
                    "action_dist_mean_entropy": torch.mean(
                        (
                            fwd_out[QF_PROBS].mean(dim=0)
                            * torch.log(fwd_out[QF_PROBS].mean(dim=0))
                        ).sum(dim=1),
                        dim=0,
                    ),
                },
                key=module_id,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )

        return total_loss
