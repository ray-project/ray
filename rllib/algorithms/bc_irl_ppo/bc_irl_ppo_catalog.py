import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.models.configs import MLPHeadConfig, MLPEncoderConfig
from ray.rllib.core.models.base import Encoder, Model


class BCIRLPPOCatalog(PPOCatalog):
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        # First initialize the super.
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        # Define the head for the reward-function here. Note, the encoder
        # needs to be defined later because at initialization
        self.rf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_vf_head_hiddens,
            hidden_layer_activation=self.pi_and_vf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=1,
        )

    def build_rf_encoder(self, framework: str) -> Encoder:

        # The input dimension reserved for the actions is either the number of
        # actions for discrete spaces (one-hot encoded) or the first shape dimension
        # for 1-dimensional Box spaces.
        required_action_dim = (
            self.action_space.shape[0]
            if isinstance(self.action_space, gym.spaces.Box)
            else self.action_space.n
        )

        # Encoder input for the reward model contains state, action, and next state. We
        # need to infer the shape for the input from the state and action spaces.
        if (
            isinstance(self.observation_space, gym.spaces.Box)
            and len(self.observation_space.shape) == 1
        ):
            input_space = gym.spaces.Box(
                -np.inf,
                np.inf,
                (self.observation_space.shape[0] * 2 + required_action_dim,),
                dtype=np.float32,
            )
        # Other observations spaces are at this moment not implemented.
        else:
            ValueError("The observation space is not supported by RLlib's BC-IRL-PPO.")

        self.rf_encoder_hiddens = self._model_config_dict["fcnet_hiddens"][:-1]
        self.rf_encoder_activation = self._model_config_dict["fcnet_activation"]

        # Now define the encoder of the reward-function.
        self.rf_encoder_config = MLPEncoderConfig(
            input_dims=input_space.shape,
            hidden_layer_dims=self.rf_encoder_hiddens,
            hidden_layer_activation=self.rf_encoder_activation,
            output_layer_dim=self.latent_dims[0],
            output_layer_activation=self.rf_encoder_activation,
        )

        # Return the built reward-function encoder.
        return self.rf_encoder_config.build(framework=framework)

    def build_rf_head(self, framework: str) -> Model:
        """Builds the reward-function head."""
        return self.rf_head_config.build(framework=framework)
