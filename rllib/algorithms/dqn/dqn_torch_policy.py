"""PyTorch policy class used for DQN."""

import logging
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import ray
from ray.rllib.algorithms.dqn.utils import make_dqn_models, postprocess_nstep_and_prio
from ray.rllib.evaluation.episode import Episode
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import (
    ComputeTDErrorMixin,
    LearningRateSchedule,
    TargetNetworkMixin,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import (
    FLOAT_MIN,
    apply_grad_clipping,
    concat_multi_gpu_td_errors,
    huber_loss,
    reduce_mean_ignore_inf,
    softmax_cross_entropy_with_logits,
)
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional
logger = logging.getLogger(__name__)


class DQNTorchPolicy(
    TargetNetworkMixin,
    ComputeTDErrorMixin,
    LearningRateSchedule,
    TorchPolicyV2,
):
    """PyTorch policy class used with SimpleQTrainer."""

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.algorithms.dqn.dqn.DQNConfig().to_dict(), **config)

        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        ComputeTDErrorMixin.__init__(self)

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

        TargetNetworkMixin.__init__(self)

    @override(TorchPolicyV2)
    def make_model(self) -> ModelV2:
        return make_dqn_models(self)

    @override(TorchPolicyV2)
    def optimizer(
        self,
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:
        # By this time, the models have been moved to the GPU - if any - and we
        # can define our optimizers using the correct CUDA variables.
        if not hasattr(self, "q_func_vars"):
            self.q_func_vars = self.model.variables()

        return torch.optim.Adam(
            self.q_func_vars, lr=self.cur_lr, eps=self.config["adam_epsilon"]
        )

    @override(TorchPolicyV2)
    def action_distribution_fn(
        self,
        model: ModelV2,
        *,
        input_dict: TensorType,
        state_batches: TensorType,
        **kwargs,
    ) -> Tuple[TensorType, type, List[TensorType]]:

        q_vals = self._compute_q_values(model, input_dict)
        q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

        model.tower_stats["q_values"] = q_vals

        return q_vals, TorchCategorical, []  # state-out

    @override(TorchPolicyV2)
    def extra_action_out(
        self,
        input_dict: Dict[str, TensorType],
        state_batches: List[TensorType],
        model: TorchModelV2,
        action_dist: TorchDistributionWrapper,
    ) -> Dict[str, TensorType]:
        return {"q_values": model.tower_stats["q_values"]}

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[Dict[Any, SampleBatch]] = None,
        episode: Optional[Episode] = None,
    ) -> SampleBatch:
        return postprocess_nstep_and_prio(
            self, sample_batch, other_agent_batches, episode
        )

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Compute loss for SimpleQ.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            The SimpleQ loss tensor given the input batch.
        """
        # Q-network evaluation.
        q_t, q_logits_t, q_probs_t, _ = self._compute_q_values(
            model,
            {"obs": train_batch[SampleBatch.CUR_OBS]},
            explore=False,
            is_training=True,
        )

        # Target Q-network evaluation.
        q_tp1, q_logits_tp1, q_probs_tp1, _ = self._compute_q_values(
            self.target_models[model],
            {"obs": train_batch[SampleBatch.NEXT_OBS]},
            explore=False,
            is_training=True,
        )

        # Q scores for actions which we know were selected in the given state.
        one_hot_selection = F.one_hot(
            train_batch[SampleBatch.ACTIONS].long(), self.action_space.n
        )
        q_t_selected = torch.sum(
            torch.where(q_t > FLOAT_MIN, q_t, torch.tensor(0.0, device=q_t.device))
            * one_hot_selection,
            1,
        )
        q_logits_t_selected = torch.sum(
            q_logits_t * torch.unsqueeze(one_hot_selection, -1), 1
        )

        # compute estimate of best possible value starting from state at t + 1
        if self.config["double_q"]:
            (
                q_tp1_using_online_net,
                q_logits_tp1_using_online_net,
                q_dist_tp1_using_online_net,
                _,
            ) = self._compute_q_values(
                model,
                {"obs": train_batch[SampleBatch.NEXT_OBS]},
                explore=False,
                is_training=True,
            )
            q_tp1_best_using_online_net = torch.argmax(q_tp1_using_online_net, 1)
            q_tp1_best_one_hot_selection = F.one_hot(
                q_tp1_best_using_online_net, self.action_space.n
            )
            q_tp1_best = torch.sum(
                torch.where(
                    q_tp1 > FLOAT_MIN, q_tp1, torch.tensor(0.0, device=q_tp1.device)
                )
                * q_tp1_best_one_hot_selection,
                1,
            )
            q_probs_tp1_best = torch.sum(
                q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1
            )
        else:
            q_tp1_best_one_hot_selection = F.one_hot(
                torch.argmax(q_tp1, 1), self.action_space.n
            )
            q_tp1_best = torch.sum(
                torch.where(
                    q_tp1 > FLOAT_MIN, q_tp1, torch.tensor(0.0, device=q_tp1.device)
                )
                * q_tp1_best_one_hot_selection,
                1,
            )
            q_probs_tp1_best = torch.sum(
                q_probs_tp1 * torch.unsqueeze(q_tp1_best_one_hot_selection, -1), 1
            )

        if self.config["num_atoms"] > 1:
            # Distributional Q-learning which corresponds to an entropy loss
            z = torch.arange(0.0, self.config["num_atoms"], dtype=torch.float32).to(
                train_batch[SampleBatch.REWARDS].device
            )
            z = self.config["v_min"] + z * (
                self.config["v_max"] - self.config["v_min"]
            ) / float(self.config["num_atoms"] - 1)

            # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
            r_tau = torch.unsqueeze(train_batch[SampleBatch.REWARDS], -1) + self.config[
                "gamma"
            ] ** self.config["n_step"] * torch.unsqueeze(
                1.0 - train_batch[SampleBatch.DONES].float(), -1
            ) * torch.unsqueeze(
                z, 0
            )
            r_tau = torch.clamp(r_tau, self.config["v_min"], self.config["v_max"])
            b = (r_tau - self.config["v_min"]) / (
                (self.config["v_max"] - self.config["v_min"])
                / float(self.config["num_atoms"] - 1)
            )
            lb = torch.floor(b)
            ub = torch.ceil(b)

            # Indispensable judgement which is missed in most implementations
            # when b happens to be an integer, lb == ub, so pr_j(s', a*) will
            # be discarded because (ub-b) == (b-lb) == 0.
            floor_equal_ceil = ((ub - lb) < 0.5).float()

            # (batch_size, num_atoms, num_atoms)
            l_project = F.one_hot(lb.long(), self.config["num_atoms"])
            # (batch_size, num_atoms, num_atoms)
            u_project = F.one_hot(ub.long(), self.config["num_atoms"])
            ml_delta = q_probs_tp1_best * (ub - b + floor_equal_ceil)
            mu_delta = q_probs_tp1_best * (b - lb)
            ml_delta = torch.sum(l_project * torch.unsqueeze(ml_delta, -1), dim=1)
            mu_delta = torch.sum(u_project * torch.unsqueeze(mu_delta, -1), dim=1)
            m = ml_delta + mu_delta

            # Rainbow paper claims that using this cross entropy loss for
            # priority is robust and insensitive to `prioritized_replay_alpha`
            self._td_error = softmax_cross_entropy_with_logits(
                logits=q_logits_t_selected, labels=m.detach()
            )
            self._q_loss = torch.mean(
                self._td_error * train_batch[SampleBatch.PRIO_WEIGHTS]
            )
            self._q_loss_stats = {
                # TODO: better Q stats for dist dqn
                "q_loss": self._q_loss,
                "td_error": self._td_error,
            }
        else:
            q_tp1_best_masked = (
                1.0 - train_batch[SampleBatch.DONES].float()
            ) * q_tp1_best

            # compute RHS of bellman equation
            q_t_selected_target = (
                train_batch[SampleBatch.REWARDS]
                + self.config["gamma"] ** self.config["n_step"] * q_tp1_best_masked
            )

            # compute the error (potentially clipped)
            self._td_error = q_t_selected - q_t_selected_target.detach()
            self._q_loss = torch.mean(
                train_batch[SampleBatch.PRIO_WEIGHTS].float()
                * huber_loss(self._td_error)
            )
            self._q_loss_stats = {
                "mean_q": torch.mean(q_t_selected),
                "min_q": torch.min(q_t_selected),
                "max_q": torch.max(q_t_selected),
                "q_loss": self._q_loss,
                "td_error": self._td_error,
            }

        # Store values for stats function in model (tower), such that for
        # multi-GPU, we do not override them during the parallel loss phase.
        model.tower_stats["td_error"] = self._td_error
        # TD-error tensor in final stats
        # will be concatenated and retrieved for each individual batch item.
        model.tower_stats["q_loss_stats"] = self._q_loss_stats

        return self._q_loss

    @override(TorchPolicyV2)
    def extra_grad_process(
        self, optimizer: "torch.optim.Optimizer", loss: TensorType
    ) -> Dict[str, TensorType]:
        # Clip grads if configured.
        return apply_grad_clipping(self, optimizer, loss)

    @override(TorchPolicyV2)
    def extra_compute_grad_fetches(self) -> Dict[str, Any]:
        fetches = convert_to_numpy(concat_multi_gpu_td_errors(self))
        return dict({LEARNER_STATS_KEY: {}}, **fetches)

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        stats = {}
        for stats_key in self.model_gpu_towers[0].tower_stats["q_loss_stats"].keys():
            stats[stats_key] = torch.mean(
                torch.stack(
                    [
                        t.tower_stats["q_loss_stats"][stats_key].to(self.device)
                        for t in self.model_gpu_towers
                        if "q_loss_stats" in t.tower_stats
                    ]
                )
            )
        stats["cur_lr"] = self.cur_lr
        return convert_to_numpy(stats)

    def _compute_q_values(
        self,
        model: ModelV2,
        input_dict,
        state_batches=None,
        seq_lens=None,
        explore=None,
        is_training: bool = False,
    ):
        model_out, state = model(input_dict, state_batches or [], seq_lens)

        if self.config["num_atoms"] > 1:
            (
                action_scores,
                z,
                support_logits_per_action,
                logits,
                probs_or_logits,
            ) = model.get_q_value_distributions(model_out)
        else:
            (action_scores, logits, probs_or_logits) = model.get_q_value_distributions(
                model_out
            )

        if self.config["dueling"]:
            state_score = model.get_state_value(model_out)
            if self.config["num_atoms"] > 1:
                support_logits_per_action_mean = torch.mean(
                    support_logits_per_action, dim=1
                )
                support_logits_per_action_centered = (
                    support_logits_per_action
                    - torch.unsqueeze(support_logits_per_action_mean, dim=1)
                )
                support_logits_per_action = (
                    torch.unsqueeze(state_score, dim=1)
                    + support_logits_per_action_centered
                )
                support_prob_per_action = nn.functional.softmax(
                    support_logits_per_action, dim=-1
                )
                value = torch.sum(z * support_prob_per_action, dim=-1)
                logits = support_logits_per_action
                probs_or_logits = support_prob_per_action
            else:
                advantages_mean = reduce_mean_ignore_inf(action_scores, 1)
                advantages_centered = action_scores - torch.unsqueeze(
                    advantages_mean, 1
                )
                value = state_score + advantages_centered
        else:
            value = action_scores

        return value, logits, probs_or_logits, state
