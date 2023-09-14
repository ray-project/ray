# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.core.models.base import ActorCriticEncoder, Encoder, Model
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import ActorCriticEncoderConfig, MLPHeadConfig
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic


class DQNCatalog(Catalog):
    """The Catalog class used to build models for DQN."""

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        """Initializes DQNCatalog."""

        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        # We can use the `ActorCriticEncoder` to hold the encoders for
        # both, Q-Network and Target-Network in DQN. The layers are not
        # shared between the two networks.
        # TODO (kourosh, sven): This is a bit confusing by name, but it
        # keeps the number of needed encoders minimal. Shall we go with it?.
        self.q_and_target_encoder_config = ActorCriticEncoderConfig(
            base_encoder_config=self._encoder_config,
            shared=False,
        )

        self.q_and_target_head_hiddens = self._model_config_dict["post_fcnet_hiddens"]
        self.q_and_target_head_activation = self._model_config_dict[
            "post_fcnet_activation"
        ]

        # For the Q-function we know the number of output nodes to be the number of
        # possible actions in the action space.
        self.q_and_target_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.q_and_target_head_hiddens,
            hidden_layer_activation=self.q_and_target_head_activation,
            output_layer_dim=action_space.n,
            output_layer_activation="linear",
        )

    @OverrideToImplementCustomLogic
    def build_q_and_target_encoder(self, framework: str) -> ActorCriticEncoder:
        """Builds the Q- and Target-network encoder."""

        return self.q_and_target_encoder_config.build(framework=framework)

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Build the encoder.

        Since DQN uses an ActorCriticEncoder for the Q- and Target-network,
        this method should not be implemented.
        """
        raise NotImplementedError(
            "Use DQNCatalog.build_q_and_target_encoder() instead for DQN."
        )

    @OverrideToImplementCustomLogic
    def build_q_head(self, framework: str) -> Model:
        """Builds the Q function head."""

        return self.q_and_target_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_target_head(self, framework: str) -> Model:
        """Builds the target Q function head."""

        return self.q_and_target_head_config.build(framework=framework)


# __sphinx_doc_end__
