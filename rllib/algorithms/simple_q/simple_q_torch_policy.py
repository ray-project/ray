"""PyTorch policy class used for Simple Q-Learning"""

import logging
from typing import Any, Dict, List, Tuple, Type, Union

import ray
from ray.rllib.algorithms.simple_q.utils import make_q_models
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import TargetNetworkMixin, LearningRateSchedule
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import concat_multi_gpu_td_errors, huber_loss
from ray.rllib.utils.typing import TensorStructType, TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional
logger = logging.getLogger(__name__)


class SimpleQTorchPolicy(
    LearningRateSchedule,
    TargetNetworkMixin,
    TorchPolicyV2,
):
    """PyTorch policy class used with SimpleQTrainer."""

    def __init__(self, observation_space, action_space, config):
        config = dict(
            ray.rllib.algorithms.simple_q.simple_q.SimpleQConfig().to_dict(), **config
        )
        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

        TargetNetworkMixin.__init__(self)

    @override(TorchPolicyV2)
    def make_model(self) -> ModelV2:
        """Builds q_model and target_model for Simple Q learning."""
        model, self.target_model = make_q_models(self)
        return model

    @override(TorchPolicyV2)
    def compute_actions(
        self,
        *,
        input_dict,
        explore=True,
        timestep=None,
        episodes=None,
        is_training=False,
        **kwargs
    ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorStructType]]:
        if timestep is None:
            timestep = self.global_timestep
        # Compute the Q-values for each possible action, using our Q-value network.
        q_vals = self._compute_q_values(
            self.model, input_dict[SampleBatch.OBS], is_training=is_training
        )
        # Use a Categorical distribution for the exploration component.
        # This way, it may either sample storchastically (e.g. when using SoftQ)
        # or deterministically/greedily (e.g. when using EpsilonGreedy).
        distribution = TorchCategorical(q_vals, self.model)
        # Call the exploration component's `get_exploration_action` method to
        # explore, if necessary.
        actions, logp = self.exploration.get_exploration_action(
            action_distribution=distribution, timestep=timestep, explore=explore
        )
        # Return (exploration) actions, state_outs (empty list), and extra outs.
        return (
            actions,
            [],
            {
                "q_values": q_vals,
                SampleBatch.ACTION_LOGP: logp,
                SampleBatch.ACTION_PROB: torch.exp(logp),
                SampleBatch.ACTION_DIST_INPUTS: q_vals,
            },
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
        target_model = self.target_models[model]

        # q network evaluation
        q_t = self._compute_q_values(
            model, train_batch[SampleBatch.CUR_OBS], is_training=True
        )

        # target q network evalution
        q_tp1 = self._compute_q_values(
            target_model,
            train_batch[SampleBatch.NEXT_OBS],
            is_training=True,
        )

        # q scores for actions which we know were selected in the given state.
        one_hot_selection = F.one_hot(
            train_batch[SampleBatch.ACTIONS].long(), self.action_space.n
        )
        q_t_selected = torch.sum(q_t * one_hot_selection, 1)

        # compute estimate of best possible value starting from state at t + 1
        dones = train_batch[SampleBatch.TERMINATEDS].float()
        q_tp1_best_one_hot_selection = F.one_hot(
            torch.argmax(q_tp1, 1), self.action_space.n
        )
        q_tp1_best = torch.sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
        q_tp1_best_masked = (1.0 - dones) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = (
            train_batch[SampleBatch.REWARDS] + self.config["gamma"] * q_tp1_best_masked
        )

        # Compute the error (Square/Huber).
        td_error = q_t_selected - q_t_selected_target.detach()
        loss = torch.mean(huber_loss(td_error))

        # Store values for stats function in model (tower), such that for
        # multi-GPU, we do not override them during the parallel loss phase.
        model.tower_stats["loss"] = loss
        # TD-error tensor in final stats
        # will be concatenated and retrieved for each individual batch item.
        model.tower_stats["td_error"] = td_error

        return loss

    @override(TorchPolicyV2)
    def extra_compute_grad_fetches(self) -> Dict[str, Any]:
        fetches = convert_to_numpy(concat_multi_gpu_td_errors(self))
        # Auto-add empty learner stats dict if needed.
        return dict({LEARNER_STATS_KEY: {}}, **fetches)

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return convert_to_numpy(
            {
                "loss": torch.mean(torch.stack(self.get_tower_stats("loss"))),
                "cur_lr": self.cur_lr,
            }
        )

    def _compute_q_values(
        self, model: ModelV2, obs_batch: TensorType, is_training=None
    ) -> TensorType:
        _is_training = is_training if is_training is not None else False
        input_dict = SampleBatch(obs=obs_batch, _is_training=_is_training)
        # Make sure, everything is PyTorch tensors.
        model_out, _ = model(input_dict, [], None)
        return model_out
