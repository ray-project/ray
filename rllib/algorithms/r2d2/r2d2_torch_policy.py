"""PyTorch policy class used for R2D2."""

from typing import Dict, List, Optional, Tuple, Type, Union

import ray
from ray.rllib.algorithms.dqn.dqn_torch_policy import DQNTorchPolicy
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import (
    FLOAT_MIN,
    huber_loss,
    sequence_mask,
)
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional


class R2D2TorchPolicy(DQNTorchPolicy):
    """PyTorch policy class used with R2D2."""

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.algorithms.r2d2.r2d2.R2D2Config().to_dict(), **config)

        DQNTorchPolicy.__init__(
            self,
            observation_space,
            action_space,
            config,
        )

    @override(DQNTorchPolicy)
    def action_distribution_fn(
        self,
        model: ModelV2,
        *,
        input_dict: TensorType,
        state_batches: TensorType,
        seq_lens: Optional[TensorType] = None,
        **kwargs,
    ) -> Tuple[TensorType, type, List[TensorType]]:

        q_vals, logits, probs_or_logits, state_out = self._compute_q_values(
            model, input_dict, state_batches, seq_lens=seq_lens
        )

        model.tower_stats["q_values"] = q_vals

        if not hasattr(self, "q_func_vars"):
            self.q_func_vars = model.variables()

        return q_vals, TorchCategorical, state_out

    @override(DQNTorchPolicy)
    def make_model(self) -> ModelV2:
        model = super().make_model()

        # Assert correct model type by checking the init state to be present.
        # For attention nets: These don't necessarily publish their init state via
        # Model.get_initial_state, but may only use the trajectory view API
        # (view_requirements).
        assert (
            model.get_initial_state() != []
            or model.view_requirements.get("state_in_0") is not None
        ), (
            "R2D2 requires its model to be a recurrent one! Try using "
            "`model.use_lstm` or `model.use_attention` in your config "
            "to auto-wrap your model with an LSTM- or attention net."
        )

        return model

    @override(DQNTorchPolicy)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Compute loss for R2D2.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            The R2D2 loss tensor given the input batch.
        """
        target_model = self.target_models[model]
        config = self.config

        # Construct internal state inputs.
        i = 0
        state_batches = []
        while "state_in_{}".format(i) in train_batch:
            state_batches.append(train_batch["state_in_{}".format(i)])
            i += 1
        assert state_batches

        # Q-network evaluation (at t).
        q, _, _, _ = DQNTorchPolicy._compute_q_values(
            self,
            model,
            train_batch,
            state_batches=state_batches,
            seq_lens=train_batch.get(SampleBatch.SEQ_LENS),
            explore=False,
            is_training=True,
        )

        # Target Q-network evaluation (at t+1).
        q_target, _, _, _ = DQNTorchPolicy._compute_q_values(
            self,
            target_model,
            train_batch,
            state_batches=state_batches,
            seq_lens=train_batch.get(SampleBatch.SEQ_LENS),
            explore=False,
            is_training=True,
        )

        actions = train_batch[SampleBatch.ACTIONS].long()
        dones = train_batch[SampleBatch.DONES].float()
        rewards = train_batch[SampleBatch.REWARDS]
        weights = train_batch[SampleBatch.PRIO_WEIGHTS]

        B = state_batches[0].shape[0]
        T = q.shape[0] // B

        # Q scores for actions which we know were selected in the given state.
        one_hot_selection = F.one_hot(actions, self.action_space.n)
        q_selected = torch.sum(
            torch.where(q > FLOAT_MIN, q, torch.tensor(0.0, device=q.device))
            * one_hot_selection,
            1,
        )

        if config["double_q"]:
            best_actions = torch.argmax(q, dim=1)
        else:
            best_actions = torch.argmax(q_target, dim=1)

        best_actions_one_hot = F.one_hot(best_actions, self.action_space.n)
        q_target_best = torch.sum(
            torch.where(
                q_target > FLOAT_MIN, q_target, torch.tensor(0.0, device=q_target.device)
            )
            * best_actions_one_hot,
            dim=1,
        )

        if config["num_atoms"] > 1:
            raise ValueError("Distributional R2D2 not supported yet!")
        else:
            q_target_best_masked_tp1 = (1.0 - dones) * torch.cat(
                [q_target_best[1:], torch.tensor([0.0], device=q_target_best.device)]
            )

            if config["use_h_function"]:
                h_inv = self._h_inverse(q_target_best_masked_tp1, config["h_function_epsilon"])
                target = self._h_function(
                    rewards + config["gamma"] ** config["n_step"] * h_inv,
                    config["h_function_epsilon"],
                )
            else:
                target = (
                    rewards + config["gamma"] ** config["n_step"] * q_target_best_masked_tp1
                )

            # Seq-mask all loss-related terms.
            seq_mask = sequence_mask(train_batch[SampleBatch.SEQ_LENS], T)[:, :-1]
            # Mask away also the burn-in sequence at the beginning.
            burn_in = self.config["replay_buffer_config"]["replay_burn_in"]
            if burn_in > 0 and burn_in < T:
                seq_mask[:, :burn_in] = False

            num_valid = torch.sum(seq_mask)

            def reduce_mean_valid(t):
                return torch.sum(t[seq_mask]) / num_valid

            # Make sure use the correct time indices:
            # Q(t) - [gamma * r + Q^(t+1)]
            q_selected = q_selected.reshape([B, T])[:, :-1]
            td_error = q_selected - target.reshape([B, T])[:, :-1].detach()
            td_error = td_error * seq_mask
            weights = weights.reshape([B, T])[:, :-1]
            total_loss = reduce_mean_valid(weights * huber_loss(td_error))

            # Store values for stats function in model (tower), such that for
            # multi-GPU, we do not override them during the parallel loss phase.
            model.tower_stats["total_loss"] = total_loss
            model.tower_stats["mean_q"] = reduce_mean_valid(q_selected)
            model.tower_stats["min_q"] = torch.min(q_selected)
            model.tower_stats["max_q"] = torch.max(q_selected)
            model.tower_stats["mean_td_error"] = reduce_mean_valid(td_error)
            # Store per time chunk (b/c we need only one mean
            # prioritized replay weight per stored sequence).
            model.tower_stats["td_error"] = torch.mean(td_error, dim=-1)

        return total_loss

    @override(DQNTorchPolicy)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return convert_to_numpy({
            "cur_lr": self.cur_lr,
            "total_loss": torch.mean(torch.stack(self.get_tower_stats("total_loss"))),
            "mean_q": torch.mean(torch.stack(self.get_tower_stats("mean_q"))),
            "min_q": torch.mean(torch.stack(self.get_tower_stats("min_q"))),
            "max_q": torch.mean(torch.stack(self.get_tower_stats("max_q"))),
            "mean_td_error": torch.mean(
                torch.stack(self.get_tower_stats("mean_td_error"))
            ),
        })

    def _h_function(self, x, epsilon=1.0):
        """h-function to normalize target Qs, described in the paper [1].

        h(x) = sign(x) * [sqrt(abs(x) + 1) - 1] + epsilon * x

        Used in [1] in combination with h_inverse:
          targets = h(r + gamma * h_inverse(Q^))
        """
        return torch.sign(x) * (torch.sqrt(torch.abs(x) + 1.0) - 1.0) + epsilon * x


    def _h_inverse(self, x, epsilon=1.0):
        """Inverse if the above h-function, described in the paper [1].

        If x > 0.0:
        h-1(x) = [2eps * x + (2eps + 1) - sqrt(4eps x + (2eps + 1)^2)] /
            (2 * eps^2)

        If x < 0.0:
        h-1(x) = [2eps * x + (2eps + 1) + sqrt(-4eps x + (2eps + 1)^2)] /
            (2 * eps^2)
        """
        two_epsilon = epsilon * 2
        if_x_pos = (
            two_epsilon * x
            + (two_epsilon + 1.0)
            - torch.sqrt(4.0 * epsilon * x + (two_epsilon + 1.0) ** 2)
        ) / (2.0 * epsilon ** 2)
        if_x_neg = (
            two_epsilon * x
            - (two_epsilon + 1.0)
            + torch.sqrt(-4.0 * epsilon * x + (two_epsilon + 1.0) ** 2)
        ) / (2.0 * epsilon ** 2)
        return torch.where(x < 0.0, if_x_neg, if_x_pos)
