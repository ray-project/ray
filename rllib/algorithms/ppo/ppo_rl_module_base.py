"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
from dataclasses import dataclass
from typing import Mapping, Any, Union

import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    MLPModelConfig,
    ActorCriticEncoderConfig,
)
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict


class PPOCatalog(Catalog):
    def __init__(self, observation_space, action_space, model_config):
        super().__init__(observation_space, action_space, model_config)
        latent_dim = self.encoder_config.output_dim

        # Replace EncoderConfig by ActorCriticEncoderConfig
        self.actor_critic_encoder_config = ActorCriticEncoderConfig(
            output_dim=latent_dim,
            base_encoder_config=self.encoder_config,
            shared=self.model_config["vf_share_layers"],
        )

        if isinstance(action_space, gym.spaces.Discrete):
            pi_output_dim = action_space.n
        else:
            pi_output_dim = action_space.shape[0] * 2

        self.pi_head_config = MLPModelConfig(
            input_dim=latent_dim, hidden_layer_dims=[32], output_dim=pi_output_dim
        )
        self.vf_head_config = MLPModelConfig(
            input_dim=latent_dim, hidden_layer_dims=[32], output_dim=1
        )

    def build_actor_critic_encoder(self, framework="torch"):
        return self.actor_critic_encoder_config.build(framework=framework)

    @override(Catalog)
    def build_encoder(self, framework="torch"):
        raise NotImplementedError("Use PPOCatalog.build_actor_critic_encoder() instead")

    def build_pi_head(self, framework="torch"):
        return self.pi_head_config.build(framework=framework)

    def build_vf_head(self, framework="torch"):
        return self.vf_head_config.build(framework=framework)


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

    def build(self, framework):
        if framework == "torch":
            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule,
            )

            return PPOTorchRLModule(self)
        else:
            from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule

            return PPOTfRLModule(self)


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    framework = None

    def __init__(self, config: RLModuleConfig):
        super().__init__()
        self.config = config
        catalog = config.catalog

        assert isinstance(catalog, PPOCatalog), "A PPOCatalog is required for PPO."

        # Create models
        self.encoder = self.config.catalog.build_actor_critic_encoder(
            framework=self.framework
        )
        self.pi = self.config.catalog.build_pi_head(framework=self.framework)
        self.vf = self.config.catalog.build_vf_head(framework=self.framework)

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config: Mapping[str, Any],
    ) -> Union["RLModule", Mapping[str, Any]]:
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

    # TODO (Artur): We use _get_initial_state here temporarily instead of
    #  get_initial_state. This should be changed back once Policy supports RNNs.
    def _get_initial_state(self) -> NestedDict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return NestedDict({})
