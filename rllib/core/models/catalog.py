import gymnasium as gym
from ray.rllib.core.models.configs import (
    MLPEncoderConfig,
    LSTMEncoderConfig,
    CNNEncoderConfig,
)
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.models import MODEL_DEFAULTS
from gymnasium.spaces import Box
from ray.rllib.models.utils import get_filter_config


class Catalog:
    """Describes the sub-modules architectures to be used in RLModules.

    RLlib's native RLModules get their Models from a Catalog object.
    By default, that Catalog builds the configs it has as attributes.
    You can modify a Catalog so that it builds different Models by subclassing and
    overriding the build_* methods. Alternatively, you can customize the configs
    inside RLlib's Catalogs to customize what is being built by RLlib.

    Usage example:

    # Define a custom catalog

    .. testcode::

        import gymnasium as gym
        from ray.rllib.core.models.configs import MLPHeadConfig
        from ray.rllib.core.models.catalog import Catalog


        class MyCatalog(Catalog):
            def __init__(
                self,
                observation_space: gym.Space,
                action_space: gym.Space,
                model_config_dict: dict,
            ):
                super().__init__(observation_space, action_space, model_config_dict)
                self.my_model_config = MLPHeadConfig(
                    hidden_layer_dims=[64, 32],
                    input_dim=self.observation_space.shape[0],
                    output_dim=1,
                )

            def build_my_head(self, framework: str):
                return self.my_model_config.build(framework=framework)

        # With that, RLlib can build and use models from this catalog like so:
        catalog = MyCatalog(gym.spaces.Box(0, 1), gym.spaces.Box(0, 1), {})
        my_head = catalog.build_my_head("torch")  # doctest: +SKIP
        out = my_head(...)  # doctest: +SKIP

        # We can also modify configs of RLlib's native Catalogs like so:
        catalog = MyCatalog(gym.spaces.Box(0, 1), gym.spaces.Box(0, 1), {})
        catalog.my_model_config.hidden_layer_dims = [32, 16]
        my_head = catalog.build_my_head("torch")  # doctest: +SKIP
        out = my_head(...)  # doctest: +SKIP
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        # TODO (Artur): Turn model_config into model_config_dict to distinguish
        #  between ModelConfig and a model_config dict.
        model_config: dict,
        view_requirements: dict = None,
    ):
        """Initializes a Catalog with a default encoder config.

        Args:
            observation_space: The observation space of the environment.
            action_space: The action space of the environment.
            model_config: The model config that specifies things like hidden
                dimensions and activations functions to use in this Catalog.
            view_requirements: The view requirements of Models to produce. This is
                needed for a Model that encodes something else than observations or
                actions but not otherwise.

        """
        self.observation_space = observation_space
        self.action_space = action_space

        # TODO (Artur): Possibly get rid of this config merge
        self.model_config = {**MODEL_DEFAULTS, **model_config}
        self.view_requirements = view_requirements

        # Produce a basic encoder config.
        self.encoder_config = self.get_encoder_config(
            observation_space=observation_space,
            action_space=action_space,
            model_config=model_config,
            view_requirements=view_requirements,
        )
        # The dimensions of the latent vector that is output by the encoder and fed
        # to the heads.
        self.latent_dim = self.encoder_config.output_dim

    def build_encoder(self, framework: str):
        """Builds the encoder.

        By default, this method builds the encoder config.

        Args:
            framework: The framework to use. Either "torch" or "tf".

        Returns:
            The encoder.
        """
        return self.encoder_config.build(framework=framework)

    def get_encoder_config(
        self,
        observation_space: gym.Space,
        model_config: dict,
        action_space: gym.Space = None,
        view_requirements=None,
    ) -> ModelConfig:
        """Returns an EncoderConfig for the given input_space and model_config.

        Encoders are usually used in RLModules to transform the input space into a
        latent space that is then fed to the heads.
        The returned EncoderConfigs relate 1:1 to the Encoder configs that RLlib
        supports. For example, for a simple 1D-Box input_space, RLlib offers an
        MLPEncoder, hence this method returns the MLPEncoderConfig. You can overwrite
        this method to produce specific EncoderConfigs for your custom Models.

        The following input spaces lead to the following configs:
        - 1D-Box: MLPEncoderConfig
        - 3D-Box: CNNEncoderConfig
        # TODO (Artur): Support more spaces here
        # ...

        Args:
            observation_space: The observation space to use.
            model_config: The model config to use.
            action_space: The action space to use if actions are to be encoded. This
                is commonly the case for LSTM models.
            view_requirements: The view requirements to use if anything else than
                observation_space or action_space is to be encoded. This signifies an
                advanced use case.

        Returns:
            The encoder config.
        """
        # TODO (Artur): Make it so that we don't work with complete MODEL_DEFAULTS
        model_config = {**MODEL_DEFAULTS, **model_config}

        activation = model_config["fcnet_activation"]
        output_activation = model_config["fcnet_activation"]
        fcnet_hiddens = model_config["fcnet_hiddens"]
        encoder_latent_dim = model_config["encoder_latent_dim"] or fcnet_hiddens[-1]

        if model_config["use_lstm"]:
            encoder_config = LSTMEncoderConfig(
                input_dim=observation_space.shape[0],
                hidden_dim=model_config["lstm_cell_size"],
                batch_first=not model_config["_time_major"],
                num_layers=1,
                output_dim=model_config["lstm_cell_size"],
                output_activation=output_activation,
                observation_space=observation_space,
                action_space=action_space,
                view_requirements_dict=view_requirements,
                get_tokenizer_encoder_config=self.get_tokenizer_encoder_config,
            )
        elif model_config["use_attention"]:
            raise NotImplementedError
        else:
            # TODO (Artur): Maybe check for original spaces here
            # input_space is a 1D Box
            if isinstance(observation_space, Box) and len(observation_space.shape) == 1:
                # In order to guarantee backward compatability with old configs,
                # we need to check if no latent dim was set and simply reuse the last
                # fcnet hidden dim for that purpose.
                if model_config["encoder_latent_dim"]:
                    hidden_layer_dims = model_config["fcnet_hiddens"]
                else:
                    hidden_layer_dims = model_config["fcnet_hiddens"][:-1]
                encoder_config = MLPEncoderConfig(
                    input_dim=observation_space.shape[0],
                    hidden_layer_dims=hidden_layer_dims,
                    hidden_layer_activation=activation,
                    output_dim=encoder_latent_dim,
                    output_activation=output_activation,
                )

            # input_space is a 3D Box
            elif (
                isinstance(observation_space, Box) and len(observation_space.shape) == 3
            ):
                if not model_config.get("conv_filters"):
                    model_config["conv_filters"] = get_filter_config(
                        observation_space.shape
                    )

                encoder_config = CNNEncoderConfig(
                    input_dims=observation_space.shape,
                    filter_specifiers=model_config["conv_filters"],
                    filter_layer_activation=activation,
                    output_activation=output_activation,
                    output_dim=encoder_latent_dim,
                )
            # input_space is a possibly nested structure of spaces.
            else:
                # NestedModelConfig
                raise NotImplementedError("No default config for complex spaces yet!")

        return encoder_config

    @classmethod
    def get_tokenizer_encoder_config(
        cls, space: gym.Space, model_config: dict
    ) -> ModelConfig:
        """Returns a tokenizer config for the given space.

        This is useful for LSTM models that need to tokenize their inputs.
        By default, RLlib uses the models supported by Catalog out of the box to
        tokenize.
        """
        return cls.get_encoder_config(
            observation_space=space,
            # Use the model_config without flags that would end up in complex models
            model_config={
                **model_config,
                **{"use_lstm": False, "use_attention": False},
            },
        )
