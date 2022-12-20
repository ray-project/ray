from gymnasium.spaces import Box, Discrete
import logging
from typing import Tuple, Type

import ray
from ray.rllib.algorithms.maml.maml_torch_policy import MAMLTorchPolicy
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import get_device

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class MBMPOTorchPolicy(MAMLTorchPolicy):
    def __init__(self, observation_space, action_space, config):
        # Validate spaces.
        # Only support single Box or single Discrete spaces.
        if not isinstance(action_space, (Box, Discrete)):
            raise UnsupportedSpaceException(
                "Action space ({}) of {} is not supported for "
                "MB-MPO. Must be [Box|Discrete].".format(action_space, self)
            )
        # If Box, make sure it's a 1D vector space.
        elif isinstance(action_space, Box) and len(action_space.shape) > 1:
            raise UnsupportedSpaceException(
                "Action space ({}) of {} has multiple dimensions "
                "{}. ".format(action_space, self, action_space.shape)
                + "Consider reshaping this into a single dimension Box space "
                "or using the multi-agent API."
            )

        config = dict(ray.rllib.algorithms.mbmpo.mbmpo.DEFAULT_CONFIG, **config)
        super().__init__(observation_space, action_space, config)

    def make_model_and_action_dist(
        self,
    ) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
        """Constructs the necessary ModelV2 and action dist class for the Policy.

        Args:
            obs_space (gym.spaces.Space): The observation space.
            action_space (gym.spaces.Space): The action space.
            config: The SAC trainer's config dict.

        Returns:
            ModelV2: The ModelV2 to be used by the Policy. Note: An additional
                target model will be created in this function and assigned to
                `policy.target_model`.
        """
        # Get the output distribution class for predicting rewards and next-obs.
        self.distr_cls_next_obs, num_outputs = ModelCatalog.get_action_dist(
            self.observation_space,
            self.config,
            dist_type="deterministic",
            framework="torch",
        )

        # Build one dynamics model if we are a Worker.
        # If we are the main MAML learner, build n (num_workers) dynamics Models
        # for being able to create checkpoints for the current state of training.
        device = get_device(self.config)

        self.dynamics_model = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            num_outputs=num_outputs,
            model_config=self.config["dynamics_model"],
            framework="torch",
            name="dynamics_ensemble",
        ).to(device)

        action_dist, num_outputs = ModelCatalog.get_action_dist(
            self.action_space, self.config, framework="torch"
        )
        # Create the pi-model and register it with the Policy.
        self.pi = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            num_outputs=num_outputs,
            model_config=self.config["model"],
            framework="torch",
            name="policy_model",
        )

        return self.pi, action_dist
