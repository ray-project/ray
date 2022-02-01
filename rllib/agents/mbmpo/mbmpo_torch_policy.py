import gym
from gym.spaces import Box, Discrete
import logging
from typing import Tuple, Type

import ray
from ray.rllib.agents.a3c.a3c_torch_policy import vf_preds_fetches
from ray.rllib.agents.maml.maml_torch_policy import (
    setup_mixins,
    maml_loss,
    maml_stats,
    maml_optimizer_fn,
    KLCoeffMixin,
)
from ray.rllib.agents.ppo.ppo_tf_policy import setup_config
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import apply_grad_clipping
from ray.rllib.utils.typing import TrainerConfigDict

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def validate_spaces(
    policy: Policy,
    observation_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Validates the observation- and action spaces used for the Policy.

    Args:
        policy (Policy): The policy, whose spaces are being validated.
        observation_space (gym.spaces.Space): The observation space to
            validate.
        action_space (gym.spaces.Space): The action space to validate.
        config (TrainerConfigDict): The Policy's config dict.

    Raises:
        UnsupportedSpaceException: If one of the spaces is not supported.
    """
    # Only support single Box or single Discrete spaces.
    if not isinstance(action_space, (Box, Discrete)):
        raise UnsupportedSpaceException(
            "Action space ({}) of {} is not supported for "
            "MB-MPO. Must be [Box|Discrete].".format(action_space, policy)
        )
    # If Box, make sure it's a 1D vector space.
    elif isinstance(action_space, Box) and len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space ({}) of {} has multiple dimensions "
            "{}. ".format(action_space, policy, action_space.shape)
            + "Consider reshaping this into a single dimension Box space "
            "or using the multi-agent API."
        )


def make_model_and_action_dist(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
    """Constructs the necessary ModelV2 and action dist class for the Policy.

    Args:
        policy (Policy): The TFPolicy that will use the models.
        obs_space (gym.spaces.Space): The observation space.
        action_space (gym.spaces.Space): The action space.
        config (TrainerConfigDict): The SAC trainer's config dict.

    Returns:
        ModelV2: The ModelV2 to be used by the Policy. Note: An additional
            target model will be created in this function and assigned to
            `policy.target_model`.
    """
    # Get the output distribution class for predicting rewards and next-obs.
    policy.distr_cls_next_obs, num_outputs = ModelCatalog.get_action_dist(
        obs_space, config, dist_type="deterministic", framework="torch"
    )

    # Build one dynamics model if we are a Worker.
    # If we are the main MAML learner, build n (num_workers) dynamics Models
    # for being able to create checkpoints for the current state of training.
    device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
    policy.dynamics_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs=num_outputs,
        model_config=config["dynamics_model"],
        framework="torch",
        name="dynamics_ensemble",
    ).to(device)

    action_dist, num_outputs = ModelCatalog.get_action_dist(
        action_space, config, framework="torch"
    )
    # Create the pi-model and register it with the Policy.
    policy.pi = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework="torch",
        name="policy_model",
    )

    return policy.pi, action_dist


# Build a child class of `TorchPolicy`, given the custom functions defined
# above.
MBMPOTorchPolicy = build_policy_class(
    name="MBMPOTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.agents.mbmpo.mbmpo.DEFAULT_CONFIG,
    make_model_and_action_dist=make_model_and_action_dist,
    loss_fn=maml_loss,
    stats_fn=maml_stats,
    optimizer_fn=maml_optimizer_fn,
    extra_action_out_fn=vf_preds_fetches,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_grad_process_fn=apply_grad_clipping,
    before_init=setup_config,
    after_init=setup_mixins,
    mixins=[KLCoeffMixin],
)
