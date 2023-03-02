"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc

import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.core.models.base import ActorCriticEncoder


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    framework = None
    default_catalog_class = PPOCatalog

    def __init__(self, spec: SingleAgentRLModuleSpec):
        super().__init__(spec=spec)
        self.spec = spec
        catalog = spec.catalog_class(
            observation_space=spec.observation_space,
            action_space=spec.action_space,
            model_config_dict=spec.model_config_dict,
        )

        assert isinstance(catalog, PPOCatalog), "A PPOCatalog is required for PPO."

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.spec.action_space),
            gym.spaces.Discrete,
        )
        assert isinstance(self.encoder, ActorCriticEncoder)
