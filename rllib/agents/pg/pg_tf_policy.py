"""
TensorFlow policy class used for PG.
"""

import gym
from typing import Dict, List, Type, Union

import ray
from ray.rllib.agents.pg.utils import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
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
    dist_inputs, _ = model.from_batch(train_batch)

    # Create an action distribution object.
    action_dist = dist_class(dist_inputs, model)

    # Calculate the vanilla PG loss based on:
    # L = -E[ log(pi(a|s)) * A]
    return -tf.reduce_mean(
        action_dist.logp(train_batch[SampleBatch.ACTIONS]) * tf.cast(
            train_batch[Postprocessing.ADVANTAGES], dtype=tf.float32))


def view_requirements_fn_pg(policy: Policy) -> \
        Dict[str, ViewRequirement]:
    """Function defining the view requirements for training/postprocessing.

    These go on top of the Policy's Model's own view requirements used for
    the action computing forward passes.

    Args:
        policy (Policy): The Policy that requires the returned
            ViewRequirements.

    Returns:
        Dict[str, ViewRequirement]: The Policy's view requirements.
    """
    ret = {
        # Next obs are needed for PPO postprocessing, but not in loss.
        SampleBatch.NEXT_OBS: ViewRequirement(
            data_col=SampleBatch.OBS,
            space=policy.observation_space,
            shift=1,
            used_for_training=False),
        # Created during postprocessing.
        Postprocessing.ADVANTAGES: ViewRequirement(shift=0),
        Postprocessing.VALUE_TARGETS: ViewRequirement(shift=0),
        # Needed for PPO's loss function.
        #SampleBatch.ACTION_DIST_INPUTS: ViewRequirement(shift=0),
        #SampleBatch.ACTION_LOGP: ViewRequirement(shift=0),
    }
    # If policy is recurrent, have to add state_out for PG-style postprocessing
    # (calculating GAE from next-obs and last state-out).
    if policy.is_recurrent():
        init_state = policy.get_initial_state()
        for i, s in enumerate(init_state):
            ret["state_out_{}".format(i)] = ViewRequirement(
                space=gym.spaces.Box(-1.0, 1.0, shape=s.shape),
                used_for_training=False)
    return ret


# Build a child class of `DynamicTFPolicy`, given the extra options:
# - trajectory post-processing function (to calculate advantages)
# - PG loss function
PGTFPolicy = build_tf_policy(
    name="PGTFPolicy",
    get_default_config=lambda: ray.rllib.agents.pg.pg.DEFAULT_CONFIG,
    loss_fn=pg_tf_loss,
    postprocess_fn=post_process_advantages,
    view_requirements_fn=view_requirements_fn_pg,
)
