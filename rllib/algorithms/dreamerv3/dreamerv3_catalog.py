import dataclasses

import gymnasium as gym

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.utils import override


@dataclasses.dataclass
class DreamerV3ModelConfig:
    model_dimension: str = None
    batch_size_B: int = None
    batch_length_T: int = None
    horizon_H: int = None
    num_gru_units: int = None
    symlog_obs: bool = None


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

        self.model_dimension = self.model_config_dict["model_dimension"]
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

            return CNNAtari(model_dimension=self.model_dimension)
        else:
            from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP

            return MLP(model_dimension=self.model_dimension)

    def build_decoder(self, framework: str) -> Model:
        """Builds the World-Model's decoder network depending on the obs space."""
        if framework != "tf2":
            raise NotImplementedError

        if self.is_img_space:
            from ray.rllib.algorithms.dreamerv3.tf.models.components.conv_transpose_atari import (
                ConvTransposeAtari,
            )

            return ConvTransposeAtari(
                model_dimension=self.model_dimension,
                gray_scaled=self.is_gray_scale,
            )
        else:
            from ray.rllib.algorithms.dreamerv3.tf.models.components.vector_decoder import (
                VectorDecoder,
            )

            return VectorDecoder(
                model_dimension=self.model_dimension,
                observation_space=self.observation_space,
            )
