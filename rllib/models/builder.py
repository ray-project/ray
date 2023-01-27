from ray.rllib.models.experimental.configs import MLPConfig, MLPEncoderConfig, LSTMEncoderConfig

def get_base_encoder_config(observation_space, model_config):
    activation = model_config["fcnet_activation"]
    if activation == "tanh":
        activation = "Tanh"
    elif activation == "relu":
        activation = "ReLU"
    elif activation == "linear":
        activation = "linear"
    else:
        raise ValueError(f"Unsupported activation: {activation}")

    obs_dim = observation_space.shape[0]
    fcnet_hiddens = model_config["fcnet_hiddens"]
    free_log_std = model_config["free_log_std"]
    assert (
        model_config.get("vf_share_layers") is False
    ), "`vf_share_layers=False` is no longer supported."

    if model_config["use_lstm"]:
        encoder_config = LSTMEncoderConfig(
            input_dim=obs_dim,
            hidden_dim=model_config["lstm_cell_size"],
            batch_first=not model_config["_time_major"],
            num_layers=1,
            output_dim=model_config["lstm_cell_size"],
        )
    else:
        encoder_config = MLPEncoderConfig(
            input_dim=obs_dim,
            hidden_layer_dims=fcnet_hiddens[:-1],
            hidden_layer_activation=activation,
            output_dim=fcnet_hiddens[-1],
        )



class ModelBuilder:
    """Defines how what models an RLModules builds.

    RLlib's native RLModules get their Models from a ModelBuilder object.
    By default, that ModelBuilder builds the configs it holds.
    You can modify the ModelBuilder so that it builds different Models by subclassing
    and don't have to write configs.
    """

    def __init__(self, observation_space, action_space, model_config):
        self.observation_space = observation_space
        self.action_space = action_space
        self.model_config = model_config

        self.base_encoder_config = get_base_encoder_config(observation_space, model_config)
        self.latent_dim = self.base_encoder_config.output_dim


    def build_encoder(self):
        return self.base_encoder_config.build()


class PPOModelBuilder(ModelBuilder):
    def __init__(self, observation_space, action_space, model_config):
        super().__init__(observation_space, action_space, model_config)
        self.pi_head_config = MLPConfig(
            input_dim=latent_dim,
            hidden_layer_dims=[32],
            output_dim=action_space.n
        )

    def build_actor_head(self):

        return self.pi_head_config.build()

    def build_critic_head(self):    def __init__(self, observation_space, action_space, model_config):
        self.vf_head_config = MLPConfig(
            input_dim=latent_dim,
            hidden_layer_dims=[32],
            output_dim=1
        )

    def build_value_head(self):
        return self.vf_head_config.build()


class DQNModelBuilder(ModelBuilder):
