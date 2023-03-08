"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc

import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.core.models.base import ActorCriticEncoder


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    framework = None

    def __init__(self, config: RLModuleConfig):
        super().__init__(config)
        catalog = self.config.get_catalog()

        assert isinstance(catalog, PPOCatalog), "A PPOCatalog is required for PPO."

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )
        assert isinstance(self.encoder, ActorCriticEncoder)
