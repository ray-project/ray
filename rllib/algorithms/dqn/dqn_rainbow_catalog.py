# __sphinx_doc_begin__
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
    """The catalog class used to build models for DQN Rainbow.

    `DQNRainbowCatalog` provides the following models:
        - Encoder: The encoder used to encode the observations.
        - Target_Encoder: The encoder used to encode the observations
            for the target network.
        - Af Head: Either the head of the advantage stream, if a dueling
            architecture is used or the head of the Q-function.
        - Vf Head (optional): The head of the value function in case a
            dueling architecture is chosen.

    All networks can include noisy layers, if `noisy` is `True`.

    Any custom head can be built by overridng the `build_af_head()` and
    `build_vf_head()`. Alternatively, the `AfHeadConfig` or `VfHeadConfig`
    can be overridden to build custom logic during `RLModule` runtime.

    All heads can optionally use distributional learning. In this case the
    number of output neurons corresponds to the number of actions times the
    number of support atoms of the discrete distribution.
    """

    @override(Catalog)
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        """Initializes the DQNRainbowCatalog.

        Args:
            observation_space: The observation space of the Encoder.
            action_space: The action space for the Af Head.
            model_config_dict: The model config to use.
        """
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
            # In case of noisy networks we need to provide the intial standard
            # deviation and use the corresponding `NoisyMLPHeadConfig`.
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

        The default behavior is to build the head from the `af_head_config`.
        This can be overridden to build a custom policy head as a means to
        configure the behavior of a `DQNRainbowRLModule` implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The advantage head in case a dueling architecutre is chosen or
            the Q-function head in the other case.
        """
        return self.af_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_vf_head(self, framework: str) -> Model:
        """Build the value function head.

        Note, this function is only called in case of a dueling architecture.

        The default behavior is to build the head from the `vf_head_config`.
        This can be overridden to build a custom policy head as a means to
        configure the behavior of a `DQNRainbowRLModule` implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The value function head.
        """

        return self.vf_head_config.build(framework=framework)

    @override(Catalog)
    def get_action_dist_cls(self, framework: str) -> "TorchCategorical":
        # We only implement DQN Rainbow for Torch.
        assert framework == "torch"
        return TorchCategorical


# __sphinx_doc_end__
