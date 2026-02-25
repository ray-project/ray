# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import _check_if_diag_gaussian
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import FreeLogStdMLPHeadConfig, MLPHeadConfig
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


class BCCatalog(Catalog):
    """The Catalog class used to build models for BC.

    BCCatalog provides the following models:
        - Encoder: The encoder used to encode the observations.
        - Pi Head: The head used for the policy logits.

    The default encoder is chosen by RLlib dependent on the observation space.
    See `ray.rllib.core.models.encoders::Encoder` for details. To define the
    network architecture use the `model_config_dict[fcnet_hiddens]` and
    `model_config_dict[fcnet_activation]`.

    To implement custom logic, override `BCCatalog.build_encoder()` or modify the
    `EncoderConfig` at `BCCatalog.encoder_config`.

    Any custom head can be built by overriding the `build_pi_head()` method.
    Alternatively, the `PiHeadConfig` can be overridden to build a custom
    policy head during runtime. To change solely the network architecture,
    `model_config_dict["head_fcnet_hiddens"]` and
    `model_config_dict["head_fcnet_activation"]` can be used.
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        """Initializes the BCCatalog.

        Args:
            observation_space: The observation space if the Encoder.
            action_space: The action space for the Pi Head.
            model_cnfig_dict: The model config to use..
        """
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        self.pi_head_hiddens = self._model_config_dict["head_fcnet_hiddens"]
        self.pi_head_activation = self._model_config_dict["head_fcnet_activation"]

        # At this time we do not have the precise (framework-specific) action
        # distribution class, i.e. we do  not know the output dimension of the
        # policy head. The config for the policy head is therefore build in the
        # `self.build_pi_head()` method.
        self.pi_head_config = None

    @OverrideToImplementCustomLogic
    def build_pi_head(self, framework: str) -> Model:
        """Builds the policy head.

        The default behavior is to build the head from the pi_head_config.
        This can be overridden to build a custom policy head as a means of configuring
        the behavior of a BC specific RLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The policy head.
        """

        # Define the output dimension via the action distribution.
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
        # With the action distribution class and the number of outputs defined,
        # we can build the config for the policy head.
        pi_head_config_cls = (
            FreeLogStdMLPHeadConfig
            if self._model_config_dict["free_log_std"]
            else MLPHeadConfig
        )
        self.pi_head_config = pi_head_config_cls(
            input_dims=self._latent_dims,
            hidden_layer_dims=self.pi_head_hiddens,
            hidden_layer_activation=self.pi_head_activation,
            output_layer_dim=required_output_dim,
            output_layer_activation="linear",
            clip_log_std=is_diag_gaussian,
            log_std_clip_param=self._model_config_dict.get("log_std_clip_param", 20),
        )

        return self.pi_head_config.build(framework=framework)


# __sphinx_doc_end__
