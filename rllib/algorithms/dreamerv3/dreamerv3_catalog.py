import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.utils import override


class DreamerV3Catalog(Catalog):
    """The Catalog class used to build all the models needed for DreamerV3 training."""

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
    ):
        """Initializes a DreamerV3Catalog instance.

        Args:
            observation_space: The observation space of the environment.
            action_space: The action space of the environment.
            model_config_dict: The model config to use.
        """
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        self.model_size = self._model_config_dict["model_size"]
        self.is_img_space = len(self.observation_space.shape) in [2, 3]
        self.is_gray_scale = (
            self.is_img_space and len(self.observation_space.shape) == 2
        )

        # TODO (sven): We should work with sub-component configurations here,
        #  and even try replacing all current Dreamer model components with
        #  our default primitives. But for now, we'll construct the DreamerV3Model
        #  directly in our `build_...()` methods.

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Builds the World-Model's encoder network depending on the obs space."""
        if framework != "tf2":
            raise NotImplementedError

        if self.is_img_space:
            from ray.rllib.algorithms.dreamerv3.tf.models.components.cnn_atari import (
                CNNAtari,
            )

            return CNNAtari(model_size=self.model_size)
        else:
            from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP

            return MLP(model_size=self.model_size, name="vector_encoder")

    def build_decoder(self, framework: str) -> Model:
        """Builds the World-Model's decoder network depending on the obs space."""
        if framework != "tf2":
            raise NotImplementedError

        if self.is_img_space:
            from ray.rllib.algorithms.dreamerv3.tf.models.components import (
                conv_transpose_atari,
            )

            return conv_transpose_atari.ConvTransposeAtari(
                model_size=self.model_size,
                gray_scaled=self.is_gray_scale,
            )
        else:
            from ray.rllib.algorithms.dreamerv3.tf.models.components import (
                vector_decoder,
            )

            return vector_decoder.VectorDecoder(
                model_size=self.model_size,
                observation_space=self.observation_space,
            )
