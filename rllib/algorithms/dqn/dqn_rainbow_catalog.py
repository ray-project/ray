import gymnasium as gym
from gymnasium.spaces import Box

from ray.rllib.algorithms.dqn.dqn_rainbow_noisy_net_configs import (
    NoisyMLPEncoderConfig,
    NoisyMLPHeadConfig,
)
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import ModelConfig
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
            architecture is used or the head of the Q-function. This is
            a multi-node head with `action_space.n` many nodes in case
            of expectation learning and `action_space.n` times the number
            of atoms (`num_atoms`) in case of distributional Q-learning.
        - Vf Head (optional): The head of the value function in case a
            dueling architecture is chosen. This is a single node head.
            If no dueling architecture is used, this head does not exist.

    All networks can include noisy layers, if `noisy` is `True`. In this
    case, no epsilon greedy exploration is used.

    Any custom head can be built by overridng the `build_af_head()` and
    `build_vf_head()`. Alternatively, the `AfHeadConfig` or `VfHeadConfig`
    can be overridden to build custom logic during `RLModule` runtime.

    All heads can optionally use distributional learning. In this case the
    number of output neurons corresponds to the number of actions times the
    number of support atoms of the discrete distribution.

    Any module built for exploration or inference is built with the flag
    `ìnference_only=True` and does not contain any target networks. This flag can
    be set in a `SingleAgentModuleSpec` through the `inference_only` boolean flag.
    Whenever the default configuration or build methods are overridden, the
    `inference_only` flag must be used with
    care to ensure that the module synching works correctly.
    The module classes contain a `_inference_only_state_dict_keys` attribute that
    contains the keys to be taken care of when synching the state. The method
    `__set_inference_only_state_dict_keys` has to be overridden to define these keys
    and `_inference_only_get_state_hook`.
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
        assert view_requirements is None, (
            "Instead, use the new ConnectorV2 API to pick whatever information "
            "you need from the running episodes"
        )

        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
        )

        # Is a noisy net used.
        self.uses_noisy: bool = self._model_config_dict["noisy"]

        # The number of atoms to be used for distributional Q-learning.
        self.num_atoms: bool = self._model_config_dict["num_atoms"]

        # Advantage and value streams have MLP heads. Note, the advantage
        # stream will has an output dimension that is the product of the
        # action space dimension and the number of atoms to approximate the
        # return distribution in distributional reinforcement learning.
        if self.uses_noisy:
            # Define the standard deviation to be used in the layers.
            self.std_init: float = self._model_config_dict["std_init"]

        # In case of noisy networks we need to provide the intial standard
        # deviation and use the corresponding `NoisyMLPHeadConfig`.
        self.af_head_config = self._get_head_config(
            output_layer_dim=int(self.action_space.n * self.num_atoms)
        )
        self.vf_head_config = self._get_head_config(output_layer_dim=1)

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
        if framework != "torch":
            raise ValueError("DQN Rainbow is only supported for framework `torch`.")
        else:
            return TorchCategorical

    @classmethod
    def _get_encoder_config(
        cls,
        observation_space: gym.Space,
        model_config_dict: dict,
        action_space: gym.Space = None,
        view_requirements=None,
    ) -> ModelConfig:
        """Returns the encoder config.

        In case noisy networks should be used, the base encoder config
        (`self._encoder_config`) is ovverriden and a `NoisyMLPEncoderConfig`
        is used.

        Note, we are overriding the default behavior of `Catalog`. Like
        this we can use the default method `build_encoder()`.

        Returns:
            If noisy networks should be used a `NoisyMLPEncoderConfig` else
            `self._encoder_config` defined by the parent class.
        """
        # Check, if we use
        use_noisy = model_config_dict["noisy"]
        use_lstm = model_config_dict["use_lstm"]
        use_attention = model_config_dict["use_attention"]

        # In cases of LSTM or Attention, fall back to the basic encoder.
        if use_noisy and not use_lstm and not use_attention:
            # Check, if the observation space is 1D Box. Only then we can use an MLP.
            if isinstance(observation_space, Box) and len(observation_space.shape) == 1:
                # Define the encoder hiddens.
                if model_config_dict["encoder_latent_dim"]:
                    af_and_vf_encoder_hiddens = model_config_dict["fcnet_hiddens"]
                    latent_dims = (model_config_dict["encoder_latent_dim"],)
                else:
                    af_and_vf_encoder_hiddens = model_config_dict["fcnet_hiddens"][:-1]
                    latent_dims = (model_config_dict["fcnet_hiddens"][-1],)

                # Instead of a regular MLP use a NoisyMLP.
                return NoisyMLPEncoderConfig(
                    input_dims=observation_space.shape,
                    hidden_layer_dims=af_and_vf_encoder_hiddens,
                    hidden_layer_activation=model_config_dict["fcnet_activation"],
                    # TODO (simon): Not yet available.
                    # hidden_layer_use_layernorm=self._model_config_dict[
                    #     "hidden_layer_use_layernorm"
                    # ],
                    # hidden_layer_use_bias=self._model_config_dict[
                    #     "hidden_layer_use_bias"
                    # ],
                    hidden_layer_weights_initializer=model_config_dict[
                        "fcnet_weights_initializer"
                    ],
                    hidden_layer_weights_initializer_config=model_config_dict[
                        "fcnet_weights_initializer_config"
                    ],
                    hidden_layer_bias_initializer=model_config_dict[
                        "fcnet_bias_initializer"
                    ],
                    hidden_layer_bias_initializer_config=model_config_dict[
                        "fcnet_bias_initializer_config"
                    ],
                    # Note, `"post_fcnet_activation"` is `"relu"` by definition.
                    output_layer_activation=model_config_dict["post_fcnet_activation"],
                    output_layer_dim=latent_dims[0],
                    # TODO (simon): Not yet available.
                    # output_layer_use_bias=self._model_config_dict[
                    #     "output_layer_use_bias"
                    # ],
                    # TODO (sven, simon): Should these initializers be rather the fcnet
                    # ones?
                    output_layer_weights_initializer=model_config_dict[
                        "post_fcnet_weights_initializer"
                    ],
                    output_layer_weights_initializer_config=model_config_dict[
                        "post_fcnet_weights_initializer_config"
                    ],
                    output_layer_bias_initializer=model_config_dict[
                        "post_fcnet_bias_initializer"
                    ],
                    output_layer_bias_initializer_config=model_config_dict[
                        "post_fcnet_bias_initializer_config"
                    ],
                    std_init=model_config_dict["std_init"],
                )
        # Otherwise return the base encoder config chosen by the parent.
        # This will choose a CNN for 3D Box and LSTM for 'use_lstm=True'.<
        return super()._get_encoder_config(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
            view_requirements=view_requirements,
        )

    def _get_head_config(self, output_layer_dim: int):
        """Returns a head config.

        Args:
            output_layer_dim: Integer defining the output layer dimension.
                This is 1 for the Vf-head and `action_space.n * num_atoms`
                for the Af(Qf)-head.

        Returns:
            In case noisy networks should be used a `NoisyMLPHeadConfig`. In
            the other case a regular `MLPHeadConfig` is returned.
        """
        # Define the config to use for the heads.
        config_cls = NoisyMLPHeadConfig if self.uses_noisy else MLPHeadConfig

        # Define the kwargs to pass in the standard deviation to the noisy network.
        kwargs = {"std_init": self.std_init} if self.uses_noisy else {}

        # Return the appropriate config.
        return config_cls(
            input_dims=self.latent_dims,
            hidden_layer_dims=self._model_config_dict["post_fcnet_hiddens"],
            # Note, `"post_fcnet_activation"` is `"relu"` by definition.
            hidden_layer_activation=self._model_config_dict["post_fcnet_activation"],
            # TODO (simon): Not yet available.
            # hidden_layer_use_layernorm=self._model_config_dict[
            #     "hidden_layer_use_layernorm"
            # ],
            # hidden_layer_use_bias=self._model_config_dict["hidden_layer_use_bias"],
            hidden_layer_weights_initializer=self._model_config_dict[
                "post_fcnet_weights_initializer"
            ],
            hidden_layer_weights_initializer_config=self._model_config_dict[
                "post_fcnet_weights_initializer_config"
            ],
            hidden_layer_bias_initializer=self._model_config_dict[
                "post_fcnet_bias_initializer"
            ],
            hidden_layer_bias_initializer_config=self._model_config_dict[
                "post_fcnet_bias_initializer_config"
            ],
            output_layer_activation="linear",
            output_layer_dim=output_layer_dim,
            # TODO (simon): Not yet available.
            # output_layer_use_bias=self._model_config_dict["output_layer_use_bias"],
            output_layer_weights_initializer=self._model_config_dict[
                "post_fcnet_weights_initializer"
            ],
            output_layer_weights_initializer_config=self._model_config_dict[
                "post_fcnet_weights_initializer_config"
            ],
            output_layer_bias_initializer=self._model_config_dict[
                "post_fcnet_bias_initializer"
            ],
            output_layer_bias_initializer_config=self._model_config_dict[
                "post_fcnet_bias_initializer_config"
            ],
            **kwargs
        )
