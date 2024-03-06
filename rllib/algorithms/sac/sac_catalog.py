import gymnasium as gym
import numpy as np

# TODO (simon): Store this function somewhere more central as many
# algorithms will use it.
from ray.rllib.algorithms.ppo.ppo_catalog import _check_if_diag_gaussian
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import (
    FreeLogStdMLPHeadConfig,
    MLPEncoderConfig,
    MLPHeadConfig,
)
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.models.torch.torch_distributions import TorchSquashedGaussian
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic


# TODO (simon): Check, if we can directly derive from DQNCatalog.
# This should work as we need a qf and qf_target.
# TODO (simon): Add CNNEnocders for Image observations.
class SACCatalog(Catalog):
    """The catalog class used to build models for SAC."""

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

        # Define the heads.
        self.pi_and_qf_head_hiddens = self._model_config_dict["post_fcnet_hiddens"]
        self.pi_and_qf_head_activation = self._model_config_dict[
            "post_fcnet_activation"
        ]

        # We don't have the exact (framework specific) action dist class yet and thus
        # cannot determine the exact number of output nodes (action space) required.
        # -> Build pi config only in the `self.build_pi_head` method.
        self.pi_head_config = None

        # TODO (simon): Implement in a later step a q network with
        # different `post_fcnet_hiddens` than pi.
        self.qf_head_config = MLPHeadConfig(
            # TODO (simon): These latent_dims could be different for the
            # q function, value function, and pi head.
            # Here we consider the simple case of identical encoders.
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=1,
        )

    @OverrideToImplementCustomLogic
    def build_qf_encoder(self, framework: str) -> Encoder:
        """Builds the Q-function encoder.

        In contrast to PPO, SAC needs a different encoder for Pi and
        Q-function as the Q-function in the continuous case has to
        encode actions, too. Therefore the Q-function uses its own
        encoder config.
        Note, the Pi network uses the base encoder from the `Catalog`.

        Args:
            framework: The framework to use. Either `torch` or `tf2`.

        Returns:
            The encoder for the Q-network.
        """

        # Compute the required dimension for the action space.
        required_action_dim = self.action_space.shape[0]

        # Encoder input for the Q-network contains state and action. We
        # need to infer the shape for the input from the state and action
        # spaces
        if (
            isinstance(self.observation_space, gym.spaces.Box)
            and len(self.observation_space.shape) == 1
        ):
            input_space = gym.spaces.Box(
                -np.inf,
                np.inf,
                (self.observation_space.shape[0] + required_action_dim,),
                dtype=np.float32,
            )
        else:
            raise ValueError("The observation space is not supported by RLlib's SAC.")

        if self._model_config_dict["encoder_latent_dim"]:
            self.qf_encoder_hiddens = self._model_config_dict["fcnet_hiddens"]
        else:
            self.qf_encoder_hiddens = self._model_config_dict["fcnet_hiddens"][:-1]

        self.qf_encoder_activation = self._model_config_dict["fcnet_activation"]

        self.qf_encoder_config = MLPEncoderConfig(
            input_dims=input_space.shape,
            hidden_layer_dims=self.qf_encoder_hiddens,
            hidden_layer_activation=self.qf_encoder_activation,
            output_layer_dim=self.latent_dims[0],
            output_layer_activation=self.qf_encoder_activation,
        )

        return self.qf_encoder_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_pi_head(self, framework: str) -> Model:
        """Builds the policy head.

        The default behavior is to build the head from the pi_head_config.
        This can be overridden to build a custom policy head as a means of configuring
        the behavior of a SACRLModule implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The policy head.
        """
        # Get action_distribution_cls to find out about the output dimension for pi_head
        action_distribution_cls = self.get_action_dist_cls(framework=framework)
        # TODO (simon): CHeck, if this holds also for Squashed Gaussian.
        if self._model_config_dict["free_log_std"]:
            _check_if_diag_gaussian(
                action_distribution_cls=action_distribution_cls, framework=framework
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
        )

        return self.pi_head_config.build(framework=framework)

    @OverrideToImplementCustomLogic
    def build_qf_head(self, framework: str) -> Model:
        """Build the Q function head."""

        return self.qf_head_config.build(framework=framework)

    @override(Catalog)
    def get_action_dist_cls(self, framework: str) -> "TorchSquashedGaussian":
        assert framework == "torch"
        return TorchSquashedGaussian
