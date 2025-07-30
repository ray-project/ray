import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.dreamerv3.utils import (
    do_symlog_obs,
    get_gru_units,
    get_num_z_classes,
    get_num_z_categoricals,
)
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
        # Compute the size of the vector coming out of the sequence model.
        self.h_plus_z_flat = get_gru_units(self.model_size) + (
            get_num_z_categoricals(self.model_size) * get_num_z_classes(self.model_size)
        )

        # TODO (sven): We should work with sub-component configurations here,
        #  and even try replacing all current Dreamer model components with
        #  our default primitives. But for now, we'll construct the DreamerV3Model
        #  directly in our `build_...()` methods.

    @override(Catalog)
    def build_encoder(self, framework: str) -> Encoder:
        """Builds the World-Model's encoder network depending on the obs space."""
        if self.is_img_space:
            if framework == "torch":
                from ray.rllib.algorithms.dreamerv3.torch.models.components import (
                    cnn_atari,
                )

                return cnn_atari.CNNAtari(
                    gray_scaled=self.is_gray_scale,
                    model_size=self.model_size,
                )
            else:
                raise ValueError(f"`framework={framework}` not supported!")

        else:
            if framework == "torch":
                from ray.rllib.algorithms.dreamerv3.torch.models.components import mlp

                return mlp.MLP(
                    input_size=int(np.prod(self.observation_space.shape)),
                    model_size=self.model_size,
                )
            else:
                raise ValueError(f"`framework={framework}` not supported!")

    def build_decoder(self, framework: str) -> Model:
        """Builds the World-Model's decoder network depending on the obs space."""

        if self.is_img_space:
            if framework == "torch":
                from ray.rllib.algorithms.dreamerv3.torch.models.components import (
                    conv_transpose_atari,
                )

                return conv_transpose_atari.ConvTransposeAtari(
                    input_size=self.h_plus_z_flat,
                    gray_scaled=self.is_gray_scale,
                    model_size=self.model_size,
                )
            else:
                raise ValueError(f"`framework={framework}` not supported!")

        else:
            if framework == "torch":
                from ray.rllib.algorithms.dreamerv3.torch.models.components import (
                    vector_decoder,
                )

                return vector_decoder.VectorDecoder(
                    input_size=self.h_plus_z_flat,
                    model_size=self.model_size,
                    observation_space=self.observation_space,
                )
            else:
                raise ValueError(f"`framework={framework}` not supported!")

    def build_world_model(self, framework: str, *, encoder, decoder) -> Model:
        symlog_obs = do_symlog_obs(
            self.observation_space,
            self._model_config_dict.get("symlog_obs", "auto"),
        )

        if framework == "torch":
            from ray.rllib.algorithms.dreamerv3.torch.models.world_model import (
                WorldModel,
            )
        else:
            raise ValueError(f"`framework={framework}` not supported!")

        return WorldModel(
            model_size=self.model_size,
            observation_space=self.observation_space,
            action_space=self.action_space,
            batch_length_T=self._model_config_dict["batch_length_T"],
            encoder=encoder,
            decoder=decoder,
            symlog_obs=symlog_obs,
        )

    def build_actor(self, framework: str) -> Model:
        if framework == "torch":
            from ray.rllib.algorithms.dreamerv3.torch.models.actor_network import (
                ActorNetwork,
            )

            return ActorNetwork(
                input_size=self.h_plus_z_flat,
                action_space=self.action_space,
                model_size=self.model_size,
            )
        else:
            raise ValueError(f"`framework={framework}` not supported!")

    def build_critic(self, framework: str) -> Model:
        if framework == "torch":
            from ray.rllib.algorithms.dreamerv3.torch.models.critic_network import (
                CriticNetwork,
            )

            return CriticNetwork(
                input_size=self.h_plus_z_flat,
                model_size=self.model_size,
            )
        else:
            raise ValueError(f"`framework={framework}` not supported!")

    def build_dreamer_model(
        self, framework: str, *, world_model, actor, critic, horizon=None, gamma=None
    ) -> Model:
        if framework == "torch":
            from ray.rllib.algorithms.dreamerv3.torch.models.dreamer_model import (
                DreamerModel,
            )
        else:
            raise ValueError(f"`framework={framework}` not supported!")

        return DreamerModel(
            model_size=self.model_size,
            action_space=self.action_space,
            world_model=world_model,
            actor=actor,
            critic=critic,
            **(
                {}
                if framework == "torch"
                else {
                    "horizon": horizon,
                    "gamma": gamma,
                }
            ),
        )
