# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import _check_if_diag_gaussian
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    FreeLogStdMLPHeadConfig,
    MLPHeadConfig,
)
from ray.rllib.core.models.base import ActorCriticEncoder, Encoder, Model
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic


class MARWILCatalog(Catalog):
    """The Catalog class used to build models for MARWIL.

    MARWILCatalog provides the following models:
        - ActorCriticEncoder: The encoder used to encode the observations.
        - Pi Head: The head used to compute the policy logits.
        - Value Function Head: The head used to compute the value function.

    The ActorCriticEncoder is a wrapper around Encoders to produce separate outputs
    for the policy and value function. See implementations of MARWILRLModule for
    more details.

    Any custom ActorCriticEncoder can be built by overriding the
    build_actor_critic_encoder() method. Alternatively, the ActorCriticEncoderConfig
    at MARWILCatalog.actor_critic_encoder_config can be overridden to build a custom
    ActorCriticEncoder during RLModule runtime.

    Any custom head can be built by overriding the build_pi_head() and build_vf_head()
    methods. Alternatively, the PiHeadConfig and VfHeadConfig can be overridden to
    build custom heads during RLModule runtime.
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        """Initializes the MARWILCatalog."""

        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        # Replace EncoderConfig by ActorCriticEncodserConfig.
        self.actor_critic_encoder_config = ActorCriticEncoderConfig(
            base_encoder_config=self._encoder_config,
            shared=self._model_config_dict["vf_share_layers"],
        )

        self.pi_and_vf_hiddens = self._model_config_dict["head_fcnet_hiddens"]
        self.pi_and_vf_activation = self._model_config_dict["head_fcnet_activation"]

        # At this time we do not have information about the exact (framework-specific)
        # action distribution class, yet. We postpone the configuration of the output
        # nodes thus to the `self.build_pi_head()` method that can be called at a state
        # where the action distribution is known.
        self.pi_head_config = None

        self.vf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_vf_hiddens,
            hidden_layer_activation=self.pi_and_vf_activation,
            output_layer_dim=1,
            output_layer_activation="linear",
        )

    @OverrideToImplementCustomLogic
    def build_actor_critic_encoder(self, framework: str) -> ActorCriticEncoder:
        """Builds the ActorCriticEncoder."""

        return self.actor_critic_encoder_config.build(framework=framework)

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Usually builds the Encoder.

        MARWIL uses a value network and therefore the ActorCriticEncoder. Therefore
        this method is not implemented and instead the
        `self.build_actor_critic_encoder()` method should be used.
        """
        raise NotImplementedError(
            "Use MARWILCatalog.build_actor_critic_encoder()` instead for MARWIL."
        )

    @OverrideToImplementCustomLogic
    def build_pi_head(self, framework: str) -> Model:
        """Builds the policy head."""

        # Get the action distribution class to define the exact output dimension.
        action_distribution_cls = self.get_action_dist_cls(framework=framework)
        if self._model_config_dict["free_log_std"]:
            _check_if_diag_gaussian(
                action_distribution_cls=action_distribution_cls, framework=framework
            )
            is_diag_gaussian = True
        else:
            is_diag_gaussian = _check_if_diag_gaussian(
                action_distribution_cls=action_distribution_cls,
                framework=framework,
                no_error=True,
            )

        required_output_dim = action_distribution_cls.required_input_dim(
            space=self.action_space, model_config=self._model_config_dict
        )
        # With the required output dimensions defined, we can build the policy head.
        pi_head_config_class = (
            FreeLogStdMLPHeadConfig
            if self._model_config_dict["free_log_std"]
            else MLPHeadConfig
        )

        self.pi_head_config = pi_head_config_class(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_vf_hiddens,
            hidden_layer_activation=self.pi_and_vf_activation,
            output_layer_dim=required_output_dim,
            output_layer_activation="linear",
            clip_log_std=is_diag_gaussian,
            log_std_clip_param=self._model_config_dict.get("log_std_clip_param", 20),
        )

        return self.pi_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_vf_head(self, framework: str) -> Model:
        """Builds the value function head."""

        return self.vf_head_config.build(framework=framework)


# __sphinx_doc_end__
