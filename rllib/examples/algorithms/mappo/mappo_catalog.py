# __sphinx_doc_begin__
import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.models.base import Encoder
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.utils import override


class MAPPOCatalog(PPOCatalog):
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
        Catalog.__init__(  # Skip PPOCatalog.__init__, since it overrides the encoder configs.
            self,
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )
        self.encoder_config = self._encoder_config
        # There is no vf head; the names below are held over from PPOCatalog.build_pi_head
        self.pi_and_vf_head_hiddens = self._model_config_dict["head_fcnet_hiddens"]
        self.pi_and_vf_head_activation = self._model_config_dict[
            "head_fcnet_activation"
        ]
        # We don't have the exact (framework specific) action dist class yet and thus
        # cannot determine the exact number of output nodes (action space) required.
        # -> Build pi config only in the `self.build_pi_head` method.
        self.pi_head_config = None

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Builds the encoder."""
        return self.encoder_config.build(framework=framework)


# __sphinx_doc_end__
