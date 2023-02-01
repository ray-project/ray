from ray.rllib.models.experimental.configs import (
    MLPModelConfig,
    MLPEncoderConfig,
    LSTMEncoderConfig,
)
from ray.rllib.models.experimental.base import ModelConfig
from ray.rllib.models.catalog import MODEL_DEFAULTS
from gymnasium.spaces import Box


class Catalog:
    """Defines how what models an RLModules builds.

    RLlib's native RLModules get their Models from a ModelBuilder object.
    By default, that ModelBuilder builds the configs it holds.
    You can modify the ModelBuilder so that it builds different Models by subclassing
    and don't have to write configs.
    """

    def __init__(self, observation_space, action_space, model_config):
        self.observation_space = observation_space
        self.action_space = action_space
        # TODO (Artur): Possibly get rid of this config merge
        self.model_config = {**MODEL_DEFAULTS, **model_config}
        self.encoder_config = self.get_encoder_config(
            observation_space, self.model_config
        )
        self.latent_dim = self.encoder_config.output_dim

    def build_encoder(self, framework="torch"):
        return self.encoder_config.build(framework=framework)

    @staticmethod
    def get_encoder_config(observation_space, model_config) -> ModelConfig:
        """Returns a Model config for the given observation space.

        Args
        """
        assert (
            len(observation_space.shape) == 1
        ), "No multidimensional obs space supported."

        activation = model_config["fcnet_activation"]
        output_activation = model_config["fcnet_activation"]
        input_dim = observation_space.shape[0]
        fcnet_hiddens = model_config["fcnet_hiddens"]

        if model_config["use_lstm"]:
            encoder_config = LSTMEncoderConfig(
                input_dim=input_dim,
                hidden_dim=model_config["lstm_cell_size"],
                batch_first=not model_config["_time_major"],
                num_layers=1,
                output_dim=model_config["lstm_cell_size"],
                output_activation=output_activation,
            )
        else:
            encoder_config = MLPEncoderConfig(
                input_dim=input_dim,
                hidden_layer_dims=fcnet_hiddens[:-1],
                hidden_layer_activation=activation,
                output_dim=fcnet_hiddens[-1],
                output_activation=output_activation,
            )
        return encoder_config

    @staticmethod
    def get_base_model_config(input_space, model_config) -> ModelConfig:
        """Returns a ModelConfig for the given input_space space.

        The returned ModelConfig can be used as is or inside an encoder.
        It is either an MLPModelConfig, a CNNModelConfig or a NestedModelConfig.
        """
        # TODO (Artur): Make it so that we don't work with complete MODEL_DEFAULTS
        model_config = {**MODEL_DEFAULTS, **model_config}
        input_dim = input_space.shape[0]

        # input_space is a 1D Box
        if isinstance(input_space, Box) and len(input_space.shape) == 1:
            # TODO (Artur): Maybe check for original spaces here
            # TODO (Artur): Discriminate between output and hidden activations
            # TODO (Artur): Maybe unify hidden_layer_dims and output_dim
            hidden_layer_dims = model_config["fcnet_hiddens"]

            activation = model_config["fcnet_activation"]
            return MLPModelConfig(
                input_dim=input_dim,
                hidden_layer_dims=hidden_layer_dims[:-1],
                hidden_layer_activation=activation,
                output_dim=hidden_layer_dims[-1],
                output_activation=activation,
            )
        # input_space is a 3D Box
        elif isinstance(input_space, Box) and len(input_space.shape) == 3:
            raise NotImplementedError("No default config for 3D spaces yet!")
        # input_space is a possibly nested structure of spaces.
        else:
            # NestedModelConfig
            raise NotImplementedError("No default config for complex spaces yet!")
