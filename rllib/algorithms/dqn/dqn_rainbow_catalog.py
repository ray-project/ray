import gymnasium as gym

from ray.rllib.algorithms.dqn.dqn_rainbow_noisy_net_configs import (
    NoisyMLPEncoderConfig,
    NoisyMLPHeadConfig,
)
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
)


@ExperimentalAPI
class DQNRainbowCatalog(Catalog):
    """The catalog class used to build models for DQN Rainbow."""

    @override(Catalog)
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        # Is a noisy net used.
        self.uses_noisy = self._model_config_dict["noisy"]
        # If a noisy network should be used.
        if self.uses_noisy:
            # TODO (simon): Add all other arguments here.
            # In this case define the encoder.
            if self._model_config_dict["encoder_latent_dim"]:
                self.af_and_vf_encoder_hiddens = self._model_config_dict[
                    "fcnet_hiddens"
                ]
            else:
                self.af_and_vf_encoder_hiddens = self._model_config_dict[
                    "fcnet_hiddens"
                ][:-1]
            self.af_and_vf_encoder_activation = self._model_config_dict[
                "fcnet_activation"
            ]
            # TODO (simon): Once the old stack is gone, rename to `std_init`.
            self.std_init = self._model_config_dict["sigma0"]

        # Define the heads.
        self.af_and_vf_head_hiddens = self._model_config_dict["post_fcnet_hiddens"]
        self.af_and_vf_head_activation = self._model_config_dict[
            "post_fcnet_activation"
        ]

        # Advantage and value streams have MLP heads. Note, the advantage
        # stream will has an output dimension that is the product of the
        # action space dimension and the number of atoms to approximate the
        # return distribution in distributional reinforcement learning.
        if self.uses_noisy:
            # Note, we are overriding the default behavior of `Catalog`. Like
            # this we can use the default method `build_encoder()`.
            self._encoder_config = NoisyMLPEncoderConfig(
                input_dims=self.observation_space.shape,
                hidden_layer_dims=self.af_and_vf_encoder_hiddens,
                hidden_layer_activation=self.af_and_vf_encoder_activation,
                output_layer_activation=self.af_and_vf_encoder_activation,
                output_layer_dim=self.latent_dims[0],
                std_init=self.std_init,
            )
        # TODO (simon): Add all other arguments to the Heads.
        if self.uses_noisy:
            self.af_head_config = NoisyMLPHeadConfig(
                input_dims=self.latent_dims,
                hidden_layer_dims=self.af_and_vf_head_hiddens,
                hidden_layer_activation=self.af_and_vf_head_activation,
                output_layer_activation="linear",
                output_layer_dim=int(
                    action_space.n * self._model_config_dict["num_atoms"]
                ),
                std_init=self.std_init,
            )
            self.vf_head_config = NoisyMLPHeadConfig(
                input_dims=self.latent_dims,
                hidden_layer_dims=self.af_and_vf_head_hiddens,
                hidden_layer_activation=self.af_and_vf_head_activation,
                output_layer_activation="linear",
                output_layer_dim=1,
                std_init=self.std_init,
            )
        else:
            self.af_head_config = MLPHeadConfig(
                input_dims=self.latent_dims,
                hidden_layer_dims=self.af_and_vf_head_hiddens,
                hidden_layer_activation=self.af_and_vf_head_activation,
                output_layer_activation="linear",
                output_layer_dim=int(
                    action_space.n * self._model_config_dict["num_atoms"]
                ),
            )
            self.vf_head_config = MLPHeadConfig(
                input_dims=self.latent_dims,
                hidden_layer_dims=self.af_and_vf_head_hiddens,
                hidden_layer_activation=self.af_and_vf_head_activation,
                output_layer_activation="linear",
                output_layer_dim=1,
            )

    @OverrideToImplementCustomLogic
    def build_af_head(self, framework: str) -> Model:
        """Build the A/Q-function head.

        Note, if no dueling architecture is chosen, this will
        be the Q-function head.
        """
        return self.af_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_vf_head(self, framework: str) -> Model:
        """Build the value function head."""

        return self.vf_head_config.build(framework=framework)

    @override(Catalog)
    def get_action_dist_cls(self, framework: str) -> "TorchCategorical":
        # We only implement for Torch.
        assert framework == "torch"
        return TorchCategorical
