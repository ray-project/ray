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

        # Set input- and output dimensions to fit PPO's needs.
        catalog.encoder_config.input_dim = self.config.observation_space.shape[0]
        catalog.pi_head_config.input_dim = catalog.encoder_config.output_dim
        if isinstance(self.config.action_space, gym.spaces.Discrete):
            catalog.pi_head_config.output_dim = int(self.config.action_space.n)
        else:
            catalog.pi_head_config.output_dim = int(
                self.config.action_space.shape[0] * 2
            )
        catalog.vf_head_config.output_dim = 1

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
        model_config: Mapping[str, Any],
    ) -> "PPORLModuleBase":
        free_log_std = model_config["free_log_std"]
        assert not free_log_std, "free_log_std not supported yet."

        if model_config["use_lstm"]:
            raise ValueError("LSTM not supported by PPOTfRLModule yet.")

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )
        catalog = PPOCatalog(
            observation_space=observation_space,
            action_space=action_space,
            model_config=model_config,
        )

        config = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            catalog=catalog,
            free_log_std=free_log_std,
        )

        return config.build(framework=cls.framework)
