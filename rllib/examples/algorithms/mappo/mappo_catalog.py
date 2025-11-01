# @title MAPPOCatalog
# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    MLPHeadConfig,
    FreeLogStdMLPHeadConfig,
)
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.utils import override
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


from ray.rllib.algorithms.ppo.ppo_catalog import _check_if_diag_gaussian


class MAPPOCatalog(Catalog):
    """The Catalog class used to build models for MAPPO.

    MAPPOCatalog provides the following models:
        - Encoder: The encoder used to encode the observations.
        - Pi Head: The head used to compute the policy logits.

    Any custom Encoder can be built by overriding the build_encoder() method. Alternatively, the EncoderConfig at MAPPOCatalog.encoder_config can be overridden to build a custom Encoder during RLModule runtime.

    Any custom head can be built by overriding the build_pi_head() method. Alternatively, the PiHeadConfig can be overridden to build a custom head during RLModule runtime.

    Any module built for exploration or inference is built with the flag `ìnference_only=True` and does not contain a value network. This flag can be set in the `SingleAgentModuleSpec` through the `inference_only` boolean flag.
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        """Initializes the MAPPOCatalog.

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
        #
        self.encoder_config = self._encoder_config
        self.pi_head_hiddens = self._model_config_dict["head_fcnet_hiddens"]
        self.pi_head_activation = self._model_config_dict["head_fcnet_activation"]
        # We don't have the exact (framework specific) action dist class yet and thus
        # cannot determine the exact number of output nodes (action space) required.
        # -> Build pi config only in the `self.build_pi_head` method.
        self.pi_head_config = None

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Builds the encoder."""
        return self.encoder_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_pi_head(self, framework: str) -> Model:
        """Builds the policy head.

        The default behavior is to build the head from the pi_head_config.
        This can be overridden to build a custom policy head as a means of configuring the behavior of a MAPPORLModule implementation.

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
        # Now that we have the action dist class and number of outputs, we can define
        # our pi-config and build the pi head.
        pi_head_config_class = (
            FreeLogStdMLPHeadConfig
            if self._model_config_dict["free_log_std"]
            else MLPHeadConfig
        )
        self.pi_head_config = pi_head_config_class(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_head_hiddens,
            hidden_layer_activation=self.pi_head_activation,
            output_layer_dim=required_output_dim,
            output_layer_activation="linear",
            clip_log_std=is_diag_gaussian,
            log_std_clip_param=self._model_config_dict.get("log_std_clip_param", 20),
        )

        return self.pi_head_config.build(framework=framework)


# __sphinx_doc_end__
