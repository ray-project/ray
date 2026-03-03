from typing import Callable

import gymnasium as gym

# TODO (simon): Store this function somewhere more central as many
# algorithms will use it.
from ray.rllib.algorithms.ppo.ppo_catalog import _check_if_diag_gaussian
from ray.rllib.core.columns import Columns
from ray.rllib.core.distribution.distribution import Distribution
from ray.rllib.core.distribution.torch.torch_distribution import (
    TorchCategorical,
    TorchSquashedGaussian,
)
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    FreeLogStdMLPHeadConfig,
    MLPEncoderConfig,
    MLPHeadConfig,
    MultiStreamEncoderConfig,
)
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic, override


# TODO (simon): Check, if we can directly derive from DQNCatalog.
# This should work as we need a qf and qf_target.
# TODO (simon): Add CNNEnocders for Image observations.
class SACCatalog(Catalog):
    """The catalog class used to build models for SAC.

    SACCatalog provides the following models:
        - Encoder: The encoder used to encode the observations for the actor
            network (`pi`). For this we use the default encoder from the Catalog.
        - Q-Function Encoder: The encoder used to encode the observations and
            actions for the soft Q-function network.
        - Target Q-Function Encoder: The encoder used to encode the observations
            and actions for the target soft Q-function network.
        - Pi Head: The head used to compute the policy logits. This network outputs
            the mean and log-std for the action distribution (a Squashed Gaussian).
        - Q-Function Head: The head used to compute the soft Q-values.
        - Target Q-Function Head: The head used to compute the target soft Q-values.

    Any custom Encoder to be used for the policy network can be built by overriding
    the build_encoder() method. Alternatively the `encoder_config` can be overridden
    by using the `model_config_dict`.

    Any custom Q-Function Encoder can be built by overriding the build_qf_encoder().
    Important: The Q-Function Encoder must encode both the state and the action. The
    same holds true for the target Q-Function Encoder.

    Any custom head can be built by overriding the build_pi_head() and build_qf_head().

    Any module built for exploration or inference is built with the flag
    `ìnference_only=True` and does not contain any Q-function. This flag can be set
    in the `model_config_dict` with the key `ray.rllib.core.rl_module.INFERENCE_ONLY`.
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        """Initializes the SACCatalog.

        Args:
            observation_space: The observation space of the Encoder.
            action_space: The action space for the Pi Head.
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
        if not isinstance(self.action_space, (gym.spaces.Box, gym.spaces.Discrete)):
            self._raise_unsupported_action_space_error()

        # Define the heads.
        self.pi_and_qf_head_hiddens = self._model_config_dict["head_fcnet_hiddens"]
        self.pi_and_qf_head_activation = self._model_config_dict[
            "head_fcnet_activation"
        ]

        # We don't have the exact (framework specific) action dist class yet and thus
        # cannot determine the exact number of output nodes (action space) required.
        # -> Build pi config only in the `self.build_pi_head` method.
        self.pi_head_config = None

        # SAC-Discrete: The Q-function outputs q-values for each action
        # SAC-Continuous: The Q-function outputs a single value (the Q-value for the
        # action taken).
        required_qf_output_dim = (
            self.action_space.n
            if isinstance(self.action_space, gym.spaces.Discrete)
            else 1
        )

        # TODO (simon): Implement in a later step a q network with
        #  different `head_fcnet_hiddens` than pi.
        # TODO (simon): These latent_dims could be different for the
        # q function, value function, and pi head.
        # Here we consider the simple case of identical encoders.
        self.qf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=required_qf_output_dim,
        )

    @OverrideToImplementCustomLogic
    def _build_qf_encoder_continuous(self, framework: str) -> Encoder:
        """Builds the Q-function encoder for continuous action spaces.

        In contrast to PPO, SAC needs a different encoder for Pi and
        Q-function as the Q-function in the continuous case has to
        encode actions, too. Therefore the Q-function uses its own
        encoder config.
        Note, the Pi network uses the base encoder from the `Catalog`.
        """
        # Configure the action encoder for the Q-function.
        self.qf_action_encoder_config = MLPEncoderConfig(
            input_dims=self.action_space.shape,
            hidden_layer_dims=self._model_config_dict["fcnet_hiddens"][:-1],
            hidden_layer_activation=self._model_config_dict["fcnet_activation"],
            output_layer_dim=self.latent_dims[0],
            output_layer_activation=self._model_config_dict["fcnet_activation"],
        )

        # Configure the Q-function encoder as a multi-stream encoder. Note that
        # the observation encoder is the same as for the policy (pi) network.
        self.qf_encoder_config = MultiStreamEncoderConfig(
            base_encoder_configs={
                Columns.OBS: self._encoder_config,
                Columns.ACTIONS: self.qf_action_encoder_config,
            },
            hidden_layer_dims=self._model_config_dict["fusionnet_hiddens"],
            hidden_layer_activation=self._model_config_dict["fusionnet_activation"],
            hidden_layer_weights_initializer=self._model_config_dict[
                "fusionnet_kernel_initializer"
            ],
            hidden_layer_weights_initializer_config=self._model_config_dict[
                "fusionnet_kernel_initializer_kwargs"
            ],
            hidden_layer_bias_initializer=self._model_config_dict[
                "fusionnet_bias_initializer"
            ],
            hidden_layer_bias_initializer_config=self._model_config_dict[
                "fusionnet_bias_initializer_kwargs"
            ],
            output_layer_dim=self.latent_dims[0],
            output_layer_activation=self._model_config_dict["fusionnet_activation"],
            output_layer_weights_initializer=self._model_config_dict[
                "fusionnet_kernel_initializer"
            ],
            output_layer_weights_initializer_config=self._model_config_dict[
                "fusionnet_kernel_initializer_kwargs"
            ],
            output_layer_bias_initializer=self._model_config_dict[
                "fusionnet_bias_initializer"
            ],
            output_layer_bias_initializer_config=self._model_config_dict[
                "fusionnet_bias_initializer_kwargs"
            ],
        )

        return self.qf_encoder_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def _build_qf_encoder_discrete(self, framework: str) -> Encoder:
        """Builds the Q-function encoder for discrete action spaces.

        In contrast to the continuous case , we don't need to encode the action
        because the Q-function will output a value for each action. Therefore,
        we can use the same encoder as for the policy (pi) network (base encoder).

        Args:
            framework: The framework to use.
        Returns:
            The encoder for the Q-network.
        """
        # For discrete action spaces, we don't need to encode the action
        # because the Q-function will output a value for each action.
        return self.build_encoder(framework=framework)

    @OverrideToImplementCustomLogic
    def build_qf_encoder(self, framework: str) -> Encoder:
        """Builds the Q-function encoder.

        In contrast to PPO, SAC needs a different encoder for Pi and
        Q-function as the Q-function in the continuous case has to
        encode actions, too. Therefore the Q-function uses its own
        encoder config.
        Note, the Pi network uses the base encoder from the `Catalog`.

        Args:
            framework: The framework to use.

        Returns:
            The encoder for the Q-network.
        """

        # Compute the required dimension for the action space.
        if isinstance(self.action_space, gym.spaces.Box):
            return self._build_qf_encoder_continuous(framework=framework)
        elif isinstance(self.action_space, gym.spaces.Discrete):
            return self._build_qf_encoder_discrete(framework=framework)
        else:
            self._raise_unsupported_action_space_error()

    @OverrideToImplementCustomLogic
    def build_pi_head(self, framework: str) -> Model:
        """Builds the policy head.

        The default behavior is to build the head from the pi_head_config.
        This can be overridden to build a custom policy head as a means of configuring
        the behavior of the DefaultSACRLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The policy head.
        """
        # Get action_distribution_cls to find out about the output dimension for pi_head
        action_distribution_cls = self.get_action_dist_cls(framework=framework)
        BUILD_MAP: dict[
            type[gym.spaces.Space], Callable[[str, Distribution], Model]
        ] = {
            gym.spaces.Discrete: self._build_pi_head_discrete,
            gym.spaces.Box: self._build_pi_head_continuous,
        }
        try:
            # Try to get the build function for the action space type.
            return BUILD_MAP[type(self.action_space)](
                framework, action_distribution_cls
            )
        except KeyError:
            # If the action space type is not supported, raise an error.
            self._raise_unsupported_action_space_error()

    def _build_pi_head_continuous(
        self, framework: str, action_distribution_cls: Distribution
    ) -> Model:
        """Builds the policy head for continuous action spaces."""
        # Get action_distribution_cls to find out about the output dimension for pi_head
        # TODO (simon): CHeck, if this holds also for Squashed Gaussian.
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
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_dim=required_output_dim,
            output_layer_activation="linear",
            clip_log_std=is_diag_gaussian,
            log_std_clip_param=self._model_config_dict.get("log_std_clip_param", 20),
        )

        return self.pi_head_config.build(framework=framework)

    def _build_pi_head_discrete(
        self, framework: str, action_distribution_cls: Distribution
    ) -> Model:
        """Builds the policy head for discrete action spaces. The module outputs logits for Categorical
        distribution.
        """
        required_output_dim = action_distribution_cls.required_input_dim(
            space=self.action_space, model_config=self._model_config_dict
        )
        self.pi_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_dim=required_output_dim,
            output_layer_activation="linear",
        )
        return self.pi_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_qf_head(self, framework: str) -> Model:
        """Build the Q function head."""

        return self.qf_head_config.build(framework=framework)

    @override(Catalog)
    def get_action_dist_cls(self, framework: str) -> Distribution:
        """Returns the action distribution class to use for the given framework. TorchSquashedGaussian
        for continuous action spaces and TorchCategorical for discrete action spaces."""
        # TODO (KIY): Catalog.get_action_dist_cls should return a type[Distribution] instead of a Distribution instance.
        assert framework == "torch"

        if isinstance(self.action_space, gym.spaces.Box):
            # For continuous action spaces, we use a Squashed Gaussian.
            return TorchSquashedGaussian
        elif isinstance(self.action_space, gym.spaces.Discrete):
            # For discrete action spaces, we use a Categorical distribution.
            return TorchCategorical
        else:
            self._raise_unsupported_action_space_error()

    def _raise_unsupported_action_space_error(self):
        """Raises an error if the action space is not supported."""
        raise ValueError(
            f"SAC only supports Box and Discrete action spaces. "
            f"Got: {type(self.action_space)}"
        )
