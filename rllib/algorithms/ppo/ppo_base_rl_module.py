"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
from typing import Mapping, Any

import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.ppo_rl_module_config import PPOModuleConfig
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.core.models.base import ActorCriticEncoder


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    framework = None

    def __init__(self, config: RLModuleConfig):
        super().__init__()
        self.config = config
        catalog = config.catalog

        assert isinstance(catalog, PPOCatalog), "A PPOCatalog is required for PPO."

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )
        assert isinstance(self.encoder, ActorCriticEncoder)

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config_dict: Mapping[str, Any],
    ) -> "PPORLModuleBase":
        catalog = PPOCatalog(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        config = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            catalog=catalog,
        )

        return config.build(framework=cls.framework)
