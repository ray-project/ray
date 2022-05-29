"""PyTorch policy class used for Simple Q-Learning"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import ray
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.policy.torch_mixins import TargetNetworkMixin
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import concat_multi_gpu_td_errors, huber_loss
from ray.rllib.utils.typing import TensorStructType, TensorType

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional
logger = logging.getLogger(__name__)


class SimpleQTorchPolicy(
    TargetNetworkMixin,
    TorchPolicyV2,
):
    """PyTorch policy class used with SimpleQTrainer."""

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.dqn.simple_q.SimpleQConfig().to_dict(), **config)
        validate_config(config)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

        TargetNetworkMixin.__init__(self)

    @override(TorchPolicyV2)
    def make_model(
        self,
    ) -> Tuple[Optional[ModelV2], Optional[Type[TorchDistributionWrapper]]]:
        return build_q_models(
            self, self.observation_space, self.action_space, self.config
        )

    @override(TorchPolicyV2)
    def compute_actions(
        self,
        *,
        input_dict: Optional[Union[SampleBatch, Dict[str, TensorStructType]]] = None,
        episodes: Optional[List["Episode"]] = None,
        explore: Optional[bool] = None,
        timestep: Optional[int] = None,
        is_training: Optional[bool] = False,
        **kwargs,
    ) -> Tuple[TensorStructType, List[TensorType], Dict[str, TensorType]]:
        q_vals = self._compute_q_values(
            self.model, input_dict[SampleBatch.OBS], is_training=False
        )
        q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

        distribution = TorchCategorical(q_vals, self.model)
        actions = self.exploration.get_exploration_action(
            action_distribution=distribution, timestep=timestep, explore=explore
        )
        return actions, [], {"q_values": q_vals}

    #@override(TorchPolicyV2)
    #def extra_action_out(
    #    self,
    #    input_dict: Dict[str, TensorType],
    #    state_batches: List[TensorType],
    #    model: TorchModelV2,
    #    action_dist: TorchDistributionWrapper,
    #) -> Dict[str, TensorType]:
    #    """Adds q-values to the action out dict."""
    #    return {"q_values": policy.q_values}

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
        target_model = policy.target_models[model]

        # q network evaluation
        q_t = compute_q_values(
            policy, model, train_batch[SampleBatch.CUR_OBS], explore=False, is_training=True
        )

        # target q network evalution
        q_tp1 = compute_q_values(
            policy,
            target_model,
            train_batch[SampleBatch.NEXT_OBS],
            explore=False,
            is_training=True,
        )

        # q scores for actions which we know were selected in the given state.
        one_hot_selection = F.one_hot(
            train_batch[SampleBatch.ACTIONS].long(), policy.action_space.n
        )
        q_t_selected = torch.sum(q_t * one_hot_selection, 1)

        # compute estimate of best possible value starting from state at t + 1
        dones = train_batch[SampleBatch.DONES].float()
        q_tp1_best_one_hot_selection = F.one_hot(
            torch.argmax(q_tp1, 1), policy.action_space.n
        )
        q_tp1_best = torch.sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
        q_tp1_best_masked = (1.0 - dones) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = (
            train_batch[SampleBatch.REWARDS] + policy.config["gamma"] * q_tp1_best_masked
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
        return {"loss": torch.mean(torch.stack(policy.get_tower_stats("loss")))}
