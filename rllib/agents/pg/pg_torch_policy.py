"""
PyTorch policy class used for PG.
"""

from typing import Dict, List, Type, Union

import ray
from ray.rllib.agents.pg.utils import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, _ = try_import_torch()


def pg_torch_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TorchDistributionWrapper],
    train_batch: SampleBatch,
) -> Union[TensorType, List[TensorType]]:
    """The basic policy gradients loss function.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]: The action distr. class.
        train_batch (SampleBatch): The training data.

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


def pg_loss_stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    """Returns the calculated loss in a stats dict.

    Args:
        policy (Policy): The Policy object.
        train_batch (SampleBatch): The data used for training.

    Returns:
        Dict[str, TensorType]: The stats dict.
    """

    return {
        "policy_loss": torch.mean(torch.stack(policy.get_tower_stats("policy_loss"))),
    }


# Build a child class of `TFPolicy`, given the extra options:
# - trajectory post-processing function (to calculate advantages)
# - PG loss function
PGTorchPolicy = build_policy_class(
    name="PGTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.agents.pg.DEFAULT_CONFIG,
    loss_fn=pg_torch_loss,
    stats_fn=pg_loss_stats,
    postprocess_fn=post_process_advantages,
)
