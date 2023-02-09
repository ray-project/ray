from dataclasses import dataclass

import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.utils.annotations import ExperimentalAPI


@ExperimentalAPI
@dataclass
class PPOModuleConfig(RLModuleConfig):
    """Configuration for the PPORLModule.

    Attributes:
        observation_space: The observation space of the environment.
        action_space: The action space of the environment.
        catalog: The PPOCatalog object to use for building the models.
        free_log_std: For DiagGaussian action distributions, make the second half of
            the model outputs floating bias variables instead of state-dependent. This
            only has an effect is using the default fully connected net.
    """

    observation_space: gym.Space = None
    action_space: gym.Space = None
    catalog: Catalog = None
    free_log_std: bool = False

    def build(self, framework: str):
        """Builds a PPORLModule.

        Args:
            framework: The framework to use for the module.

        Returns:
            PPORLModule: The module.
        """
        if framework == "torch":
            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule,
            )

            return PPOTorchRLModule(self)
        else:
            from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule

            return PPOTfRLModule(self)