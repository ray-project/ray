from typing import Dict, List, Optional, Type, Union

import ray
from ray.rllib.agents.pg.utils import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.types import TensorType

torch, _ = try_import_torch()


def pg_torch_loss(
        policy: Policy,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch) -> Union[TensorType, List[TensorType]]:
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
    logits, _ = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)
    log_probs = action_dist.logp(train_batch[SampleBatch.ACTIONS])
    # Save the error in the policy object.
    # policy.pi_err = -train_batch[Postprocessing.ADVANTAGES].dot(
    # log_probs.reshape(-1)) / len(log_probs)
    policy.pi_err = -torch.mean(
        log_probs * train_batch[Postprocessing.ADVANTAGES])
    return policy.pi_err


def pg_loss_stats(
        policy: Policy,
        train_batch: SampleBatch) -> Dict[str, TensorType]:

    """Returns the calculated loss in a stats dict.

    Args:
        policy (Policy): The Policy object.
        train_batch (SampleBatch): The data used for training.

    Returns:
        Dict[str, TensorType]: The stats dict.
    """

    return {
        # `pi_err` is stored when computing the loss.
        "policy_loss": policy.pi_err.item(),
    }


# Build a child class of `TFPolicy`, given the extra options:
# - trajectory post-processing function (to calculate advantages)
# - PG loss function
PGTorchPolicy = build_torch_policy(
    name="PGTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.pg.pg.DEFAULT_CONFIG,
    loss_fn=pg_torch_loss,
    stats_fn=pg_loss_stats,
    postprocess_fn=post_process_advantages)
