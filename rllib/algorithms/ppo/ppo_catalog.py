# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    MLPHeadConfig,
    FreeLogStdMLPHeadConfig,
)
from ray.rllib.core.models.base import Encoder, ActorCriticEncoder, Model
from ray.rllib.utils import override
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


def _check_if_diag_gaussian(action_distribution_cls, framework):
    if framework == "torch":
        from ray.rllib.models.torch.torch_distributions import TorchDiagGaussian

        assert issubclass(action_distribution_cls, TorchDiagGaussian), (
            f"free_log_std is only supported for DiagGaussian action distributions. "
            f"Found action distribution: {action_distribution_cls}."
        )
    elif framework == "tf2":
        from ray.rllib.models.tf.tf_distributions import TfDiagGaussian

        assert issubclass(action_distribution_cls, TfDiagGaussian), (
            "free_log_std is only supported for DiagGaussian action distributions. "
            "Found action distribution: {}.".format(action_distribution_cls)
        )
    else:
        raise ValueError(f"Framework {framework} not supported for free_log_std.")


class PPOCatalog(Catalog):
    """The Catalog class used to build models for PPO.

    PPOCatalog provides the following models:
        - ActorCriticEncoder: The encoder used to encode the observations.
        - Pi Head: The head used to compute the policy logits.
        - Value Function Head: The head used to compute the value function.

    The ActorCriticEncoder is a wrapper around Encoders to produce separate outputs
    for the policy and value function. See implementations of PPORLModule for
    more details.

    Any custom ActorCriticEncoder can be built by overriding the
    build_actor_critic_encoder() method. Alternatively, the ActorCriticEncoderConfig
    at PPOCatalog.actor_critic_encoder_config can be overridden to build a custom
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
        """Initializes the PPOCatalog.

        Args:
            observation_space: The observation space of the Encoder.
            action_space: The action space for the Pi Head.
            model_config_dict: The model config to use.
        """
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        # Replace EncoderConfig by ActorCriticEncoderConfig
        self.actor_critic_encoder_config = ActorCriticEncoderConfig(
            base_encoder_config=self._encoder_config,
            shared=self._model_config_dict["vf_share_layers"],
        )

        self.pi_and_vf_head_hiddens = self._model_config_dict["post_fcnet_hiddens"]
        self.pi_and_vf_head_activation = self._model_config_dict[
            "post_fcnet_activation"
        ]

        # We don't have the exact (framework specific) action dist class yet and thus
        # cannot determine the exact number of output nodes (action space) required.
        # -> Build pi config only in the `self.build_pi_head` method.
        self.pi_head_config = None

        self.vf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_vf_head_hiddens,
            hidden_layer_activation=self.pi_and_vf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=1,
        )

    @OverrideToImplementCustomLogic
    def build_actor_critic_encoder(self, framework: str) -> ActorCriticEncoder:
        """Builds the ActorCriticEncoder.

        The default behavior is to build the encoder from the encoder_config.
        This can be overridden to build a custom ActorCriticEncoder as a means of
        configuring the behavior of a PPORLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The ActorCriticEncoder.
        """
        return self.actor_critic_encoder_config.build(framework=framework)

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Builds the encoder.

        Since PPO uses an ActorCriticEncoder, this method should not be implemented.
        """
        raise NotImplementedError(
            "Use PPOCatalog.build_actor_critic_encoder() instead for PPO."
        )

    @OverrideToImplementCustomLogic
    def build_pi_head(self, framework: str) -> Model:
        """Builds the policy head.

        The default behavior is to build the head from the pi_head_config.
        This can be overridden to build a custom policy head as a means of configuring
        the behavior of a PPORLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The policy head.
        """
        # Get action_distribution_cls to find out about the output dimension for pi_head
        action_distribution_cls = self.get_action_dist_cls(framework=framework)
        if self._model_config_dict["free_log_std"]:
            _check_if_diag_gaussian(
                action_distribution_cls=action_distribution_cls, framework=framework
            )
        required_output_dim = action_distribution_cls.required_input_dim(
            space=self.action_space, model_config=self._model_config_dict
        )
        # Now that we have the action dist class and number of outputs, we can define
        # our pi-config and build the pi head.
        pi_head_config_class = (
            FreeLogStdMLPHeadConfig
            if self._model_config_dict["free_log_std"]
            else MLPHeadConfig
        )
        self.pi_head_config = pi_head_config_class(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_vf_head_hiddens,
            hidden_layer_activation=self.pi_and_vf_head_activation,
            output_layer_dim=required_output_dim,
            output_layer_activation="linear",
        )

        return self.pi_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_vf_head(self, framework: str) -> Model:
        """Builds the value function head.

        The default behavior is to build the head from the vf_head_config.
        This can be overridden to build a custom value function head as a means of
        configuring the behavior of a PPORLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The value function head.
        """
        return self.vf_head_config.build(framework=framework)


# __sphinx_doc_end__
