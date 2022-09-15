"""
PyTorch policy class used for PG.
"""
import logging
from typing import Dict, List, Type, Union, Optional, Tuple

import ray

from ray.rllib.evaluation.episode import Episode
from ray.rllib.utils.typing import AgentID
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.algorithms.pg.utils import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import LearningRateSchedule
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class PGTorchPolicy(LearningRateSchedule, TorchPolicyV2):
    """PyTorch policy class used with PGTrainer."""

    def __init__(self, observation_space, action_space, config):

        config = dict(ray.rllib.algorithms.pg.PGConfig().to_dict(), **config)

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

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """The basic policy gradients loss function.

        Calculates the vanilla policy gradient loss based on:
        L = -E[ log(pi(a|s)) * A]

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            Union[TensorType, List[TensorType]]: A single loss tensor or a list
                of loss tensors.
        """
        # Pass the training data through our model to get distribution parameters.
        dist_inputs, _ = model(train_batch)

        # Create an action distribution object.
        action_dist = dist_class(dist_inputs, model)

        # Calculate the vanilla PG loss based on:
        # L = -E[ log(pi(a|s)) * A]
        log_probs = action_dist.logp(train_batch[SampleBatch.ACTIONS])

        # Final policy loss.
        policy_loss = -torch.mean(log_probs * train_batch[Postprocessing.ADVANTAGES])

        # Store values for stats function in model (tower), such that for
        # multi-GPU, we do not override them during the parallel loss phase.
        model.tower_stats["policy_loss"] = policy_loss

        return policy_loss

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        """Returns the calculated loss in a stats dict.

        Args:
            policy: The Policy object.
            train_batch: The data used for training.

        Returns:
            Dict[str, TensorType]: The stats dict.
        """

        return convert_to_numpy(
            {
                "policy_loss": torch.mean(
                    torch.stack(self.get_tower_stats("policy_loss"))
                ),
                "cur_lr": self.cur_lr,
            }
        )

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[
            Dict[AgentID, Tuple["Policy", SampleBatch]]
        ] = None,
        episode: Optional["Episode"] = None,
    ) -> SampleBatch:
        sample_batch = super().postprocess_trajectory(
            sample_batch, other_agent_batches, episode
        )
        return post_process_advantages(self, sample_batch, other_agent_batches, episode)
