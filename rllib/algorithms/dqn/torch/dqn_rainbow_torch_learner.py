from typing import Mapping

from ray.rllib.algorithms.dqn.dqn import DQNConfig
from ray.rllib.algorithms.dqn.dqn_rainbow_learner import (
    ATOMS,
    DQNRainbowLearner,
    QF_LOSS_KEY,
    QF_LOGITS,
    QF_MEAN_KEY,
    QF_MAX_KEY,
    QF_MIN_KEY,
    QF_TARGET_NEXT_PREDS,
    QF_TARGET_NEXT_PROBS,
    QF_PREDS,
    QF_PROBS,
    TD_ERROR_KEY,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import ModuleID, TensorType


torch, nn = try_import_torch()


class DQNRainbowTorchLearner(DQNRainbowLearner, TorchLearner):
    """Implements `torch`-specific DQN Rainbow loss logic on top of `DQNRainbowLearner`

    This ' Learner' class implements the loss in its
    `self.compute_loss_for_module()` method.
    """

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: DQNConfig,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:

        q_curr = fwd_out[QF_PREDS]
        q_target_next = fwd_out[QF_TARGET_NEXT_PREDS]

        # Get the Q-values for the selected actions in the rollout.
        # TODO (simon): Check, if we can use `gather` with a complex action
        # space - we might need the one_hot_selection. Also test performance.
        q_selected = torch.nan_to_num(
            torch.gather(
                q_curr,
                dim=1,
                index=batch[SampleBatch.ACTIONS].long(),
            ),
            neginf=0.0,
        )

        # Use double Q learning.
        if self.config.double_q:
            # Then we evaluate the target Q-function at the best action (greedy action)
            # over the online Q-function.
            batch_next = {SampleBatch.OBS: batch[SampleBatch.NEXT_OBS]}
            qf_next_outs = self.module.qf(batch_next)
            # Mark the best online Q-value of the next state.
            q_next_best_idx = (
                torch.argmax(qf_next_outs[QF_PREDS], dim=1).unsqueeze(dim=-1).long()
            )
            # Get the Q-value of the target network at maximum of the online network
            # (bootstrap action).
            q_next_best = torch.nan_to_num(
                torch.gather(q_target_next, dim=1, index=q_next_best_idx),
                neginf=0.0,
            )
        else:
            # Mark the maximum Q-value(s).
            q_next_best_idx = (
                torch.argmax(q_target_next, dim=1).unsqueeze(dim=-1).long()
            )
            # Get the maximum Q-value(s).
            q_next_best = torch.nan_to_num(
                torch.gather(q_target_next, dim=1, index=q_next_best_idx),
                neginf=0.0,
            )

        # Choose the requested loss function. Note, in case of the Huber loss
        # we fall back to the default of `delta=1.0`.
        loss_fn = (
            nn.HuberLoss if self.config.td_error_loss_fn == "huber" else nn.MSELoss
        )

        # If we learn a Q-distribution.
        if self.config.num_atoms > 1:
            # Extract the Q-logits evaluated at the selected actions.
            # (Note, `torch.gather` should be faster than multiplication
            # with a one-hot tensor.)
            q_logits_selected = torch.gather(
                fwd_out[QF_LOGITS],
                dim=1,
                # Note, the Q-logits are of shape (B, action_space.n, num_atoms)
                # while the actions have shape (B, 1). We reshape actions to
                # (B, 1, num_atoms).
                index=batch[SampleBatch.ACTIONS]
                .view(-1, 1, 1)
                .expand(-1, 1, self.config.num_atoms)
                .long(),
            ).squeeze(dim=1)
            # Get the probabilies for the maximum Q-value(s).
            probs_q_next_best = torch.gather(
                fwd_out[QF_TARGET_NEXT_PROBS],
                dim=1,
                # Change the view and then expand to get to the dimensions
                # of the probabilities (dims 0 and 2, 1 should be reduced
                # from 2 -> 1).
                index=q_next_best_idx.view(-1, 1, 1).expand(
                    -1, 1, self.config.num_atoms
                ),
            ).squeeze(dim=1)

            # For distributional Q-learning we use an entropy loss.

            # Extract the support grid for the Q distribution.
            z = fwd_out[ATOMS]
            # TODO (simon): Enable computing on GPU.
            # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
            # TODO (simon): Check, if we need to unsqueeze here.
            r_tau = torch.clamp(
                batch[SampleBatch.REWARDS]
                + self.config.gamma ** batch["n_steps"]
                * (1.0 - batch[SampleBatch.TERMINATEDS].float())
                * z,
                self.config.v_min,
                self.config.v_max,
            )
            b = (r_tau - self.config.v_min) / (
                (self.config.v_max - self.config.v_min)
                / float(self.config.num_atoms - 1.0)
            )
            lower_bound = torch.floor(b)
            upper_bound = torch.ceil(b)

            floor_equal_ceil = ((upper_bound - lower_bound) < 0.5).float()

            # (B, num_atoms, num_atoms).
            lower_projection = nn.functional.one_hot(
                lower_bound.long(), self.config.num_atoms
            )
            upper_projection = nn.functional.one_hot(
                upper_bound.long(), self.config.num_atoms
            )
            ml_delta = probs_q_next_best * (upper_bound - b + floor_equal_ceil)
            mu_delta = probs_q_next_best * (b - lower_bound)
            ml_delta = torch.sum(lower_projection * ml_delta.unsqueeze(dim=-1), dim=1)
            mu_delta = torch.sum(upper_projection * mu_delta.unsqueeze(dim=-1), dim=1)
            # We do not want to propagate through the distributional targets.
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
                1.0 - batch[SampleBatch.TERMINATEDS].float()
            ) * q_next_best

            # Compute the RHS of the Bellman equation.
            # Detach this node from the computation graph as we do not want to
            # backpropagate through the target network when optimizing the Q loss.
            q_selected_target = (
                batch[SampleBatch.REWARDS]
                + self.config.gamma ** batch["n_steps"] * q_next_best_masked
            ).detach()

            # Compute the TD error.
            td_error = torch.abs(q_selected - q_selected_target)
            # Compute the weighted loss (importance sampling weights).
            total_loss = torch.mean(
                batch["weights"]
                * loss_fn(reduction="none")(q_selected, q_selected_target)
            )

        self.register_metrics(
            module_id,
            {
                QF_LOSS_KEY: total_loss,
                TD_ERROR_KEY: td_error,
                QF_MEAN_KEY: torch.mean(q_selected),
                QF_MAX_KEY: torch.max(q_selected),
                QF_MIN_KEY: torch.min(q_selected),
            },
        )
        # If we learn a Q-value distribution store the support and average
        # probabilities.
        if self.config.num_atoms > 1:
            self.register_metrics(
                module_id,
                {
                    ATOMS: z,
                    QF_PROBS: torch.mean(fwd_out[QF_PROBS], dim=0),
                },
            )

        return total_loss

    @override(DQNRainbowLearner)
    def _update_module_target_networks(
        self, module_id: ModuleID, config: DQNConfig
    ) -> None:
        module = self.module[module_id]

        # Note, we have pairs of encoder and head networks.
        target_current_network_pairs = module.get_target_network_pairs()
        for target_network, current_network in target_current_network_pairs:
            # Get the current parameters from the Q network.
            current_state_dict = current_network.state_dict()
            # Use here Polyak averaging.
            new_state_dict = {
                k: config.tau * current_state_dict[k] + (1 - config.tau) * v
                for k, v in target_network.state_dict().items()
            }
            # Apply the new parameters to the target Q network.
            target_network.load_state_dict(new_state_dict)
