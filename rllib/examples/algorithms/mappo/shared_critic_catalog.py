import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    MLPHeadConfig,
)
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.utils import override
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


class SharedCriticCatalog(Catalog):
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,  # TODO: Remove?
        model_config_dict: dict,
    ):
        """Initializes the PPOCatalog.

        Args:
            observation_space: The observation space of the Encoder.
            action_space: The action space for the Pi Head.
            model_config_dict: The model config to use.
        """
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,  # Base Catalog class checks for this.
            model_config_dict=model_config_dict,
        )
        # We only want one encoder, so we use the base encoder config.
        self.encoder_config = self._encoder_config
        # Adjust the input and output dimensions of the encoder.
        observation_spaces = self._model_config_dict["observation_spaces"]
        obs_size = 0
        self.encoder_config.output_dim = len(observation_spaces)
        for agent, obs in observation_spaces.items():
            obs_size += obs.shape[0]  # Assume 1D observations
        self.encoder_config.input_dims = (obs_size,)
        # Value head architecture
        self.vf_head_hiddens = self._model_config_dict["head_fcnet_hiddens"]
        self.vf_head_activation = self._model_config_dict["head_fcnet_activation"]
        self.vf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.vf_head_hiddens,
            hidden_layer_activation=self.vf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=len(observation_spaces),  # 1 value pred. per agent
        )

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Builds the encoder."""
        return self.encoder_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_vf_head(self, framework: str) -> Model:
        """Builds the value function head.

        The default behavior is to build the head from the vf_head_config.
        This can be overridden to build a custom value function head as a means of
        configuring the behavior of a MAPPORLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The value function head.
        """
        return self.vf_head_config.build(framework=framework)


# __sphinx_doc_end__
