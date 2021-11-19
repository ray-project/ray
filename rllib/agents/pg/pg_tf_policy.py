"""
TensorFlow policy class used for PG.
"""

from typing import Dict, List, Type, Union

import ray
from ray.rllib.agents.pg.utils import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()


def pg_tf_loss(
        policy: Policy, model: ModelV2, dist_class: Type[ActionDistribution],
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
    # Pass the training data through our model to get distribution parameters.
    dist_inputs, _ = model(train_batch)

    # Create an action distribution object.
    action_dist = dist_class(dist_inputs, model)

    # Calculate the vanilla PG loss based on:
    # L = -E[ log(pi(a|s)) * A]
    loss = -tf.reduce_mean(
        action_dist.logp(train_batch[SampleBatch.ACTIONS]) * tf.cast(
            train_batch[Postprocessing.ADVANTAGES], dtype=tf.float32))

    policy.policy_loss = loss

    return loss


def pg_loss_stats(policy: Policy,
                  train_batch: SampleBatch) -> Dict[str, TensorType]:
    """Returns the calculated loss in a stats dict.

    Args:
        policy (Policy): The Policy object.
        train_batch (SampleBatch): The data used for training.

    Returns:
        Dict[str, TensorType]: The stats dict.
    """

    return {
        "policy_loss": policy.policy_loss,
    }


# Build a child class of `DynamicTFPolicy`, given the extra options:
# - trajectory post-processing function (to calculate advantages)
# - PG loss function
PGTFPolicy = build_tf_policy(
    name="PGTFPolicy",
    get_default_config=lambda: ray.rllib.agents.pg.DEFAULT_CONFIG,
    postprocess_fn=post_process_advantages,
    stats_fn=pg_loss_stats,
    loss_fn=pg_tf_loss)
