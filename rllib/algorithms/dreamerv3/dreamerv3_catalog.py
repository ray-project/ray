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
        model_config: DreamerV3ModelConfig,
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
            # TODO (sven): Make all catalogs only support ModelConfig objects.
            model_config_dict=dataclasses.asdict(model_config),
        )

        self.model_config = model_config
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

        from ray.rllib.algorithms.dreamerv3.tf.models.components.cnn_atari import (
            CNNAtari,
        )
        from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP

        encoder = (
            CNNAtari(model_dimension=self.model_config.model_dimension)
            if self.is_img_space
            else MLP(model_dimension=self.model_config.model_dimension)
        )
        return encoder

    def build_decoder(self, framework: str) -> Model:
        """Builds the World-Model's decoder network depending on the obs space."""
        if framework != "tf2":
            raise NotImplementedError

        from ray.rllib.algorithms.dreamerv3.tf.models.components.conv_transpose_atari import (
            ConvTransposeAtari,
        )
        from ray.rllib.algorithms.dreamerv3.tf.models.components.vector_decoder import (
            VectorDecoder,
        )

        decoder = (
            ConvTransposeAtari(
                model_dimension=self.model_config.model_dimension,
                gray_scaled=self.is_gray_scale,
            )
            if self.is_img_space
            else VectorDecoder(
                model_dimension=self.model_config.model_dimension,
                observation_space=self.observation_space,
            )
        )
        return decoder

    def build_world_model(self, framework: str) -> Model:
        if framework != "tf2":
            raise NotImplementedError

        from ray.rllib.algorithms.dreamerv3.tf.models.world_model import WorldModel

        encoder = self.build_encoder(framework=framework)
        decoder = self.build_decoder(framework=framework)
        world_model = WorldModel(
            model_dimension=self.model_config.model_dimension,
            action_space=self.action_space,
            batch_length_T=self.model_config.batch_length_T,
            num_gru_units=self.model_config.num_gru_units,
            encoder=encoder,
            decoder=decoder,
            symlog_obs=self.model_config.symlog_obs,
        )
        return world_model

    def build_dreamer_model(self, framework: str) -> Model:
        if framework != "tf2":
            raise NotImplementedError

        from ray.rllib.algorithms.dreamerv3.tf.models.dreamer_model import DreamerModel

        world_model = self.build_world_model(framework=framework)
        dreamer_model = DreamerModel(
            model_dimension=self.model_config.model_dimension,
            action_space=self.action_space,
            world_model=world_model,
            # use_curiosity=use_curiosity,
            # intrinsic_rewards_scale=intrinsic_rewards_scale,
            batch_size_B=self.model_config.batch_size_B,
            batch_length_T=self.model_config.batch_length_T,
            horizon_H=self.model_config.horizon_H,
        )
        return dreamer_model
