import gymnasium as gym

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
class DQNCatalog(Catalog):
    """The catalog class used to build models for DQN Rainbow.

    `DQNCatalog` provides the following models:
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

    Any custom head can be built by overridng the `build_af_head()` and
    `build_vf_head()`. Alternatively, the `AfHeadConfig` or `VfHeadConfig`
    can be overridden to build custom logic during `RLModule` runtime.

    All heads can optionally use distributional learning. In this case the
    number of output neurons corresponds to the number of actions times the
    number of support atoms of the discrete distribution.

    Any module built for exploration or inference is built with the flag
    `ìnference_only=True` and does not contain any target networks. This flag can
    be set in a `SingleAgentModuleSpec` through the `inference_only` boolean flag.
    """

    @override(Catalog)
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        """Initializes the DQNCatalog.

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

        # The number of atoms to be used for distributional Q-learning.
        self.num_atoms: bool = self._model_config_dict["num_atoms"]

        # Advantage and value streams have MLP heads. Note, the advantage
        # stream will has an output dimension that is the product of the
        # action space dimension and the number of atoms to approximate the
        # return distribution in distributional reinforcement learning.
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
        configure the behavior of a `DQNRLModule` implementation.

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
        configure the behavior of a `DQNRLModule` implementation.

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

    def _get_head_config(self, output_layer_dim: int):
        """Returns a head config.

        Args:
            output_layer_dim: Integer defining the output layer dimension.
                This is 1 for the Vf-head and `action_space.n * num_atoms`
                for the Af(Qf)-head.

        Returns:
            A `MLPHeadConfig`.
        """
        # Return the appropriate config.
        return MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self._model_config_dict["head_fcnet_hiddens"],
            # Note, `"post_fcnet_activation"` is `"relu"` by definition.
            hidden_layer_activation=self._model_config_dict["head_fcnet_activation"],
            # TODO (simon): Not yet available.
            # hidden_layer_use_layernorm=self._model_config_dict[
            #     "hidden_layer_use_layernorm"
            # ],
            # hidden_layer_use_bias=self._model_config_dict["hidden_layer_use_bias"],
            hidden_layer_weights_initializer=self._model_config_dict[
                "head_fcnet_kernel_initializer"
            ],
            hidden_layer_weights_initializer_config=self._model_config_dict[
                "head_fcnet_kernel_initializer_kwargs"
            ],
            hidden_layer_bias_initializer=self._model_config_dict[
                "head_fcnet_bias_initializer"
            ],
            hidden_layer_bias_initializer_config=self._model_config_dict[
                "head_fcnet_bias_initializer_kwargs"
            ],
            output_layer_activation="linear",
            output_layer_dim=output_layer_dim,
            # TODO (simon): Not yet available.
            # output_layer_use_bias=self._model_config_dict["output_layer_use_bias"],
            output_layer_weights_initializer=self._model_config_dict[
                "head_fcnet_kernel_initializer"
            ],
            output_layer_weights_initializer_config=self._model_config_dict[
                "head_fcnet_kernel_initializer_kwargs"
            ],
            output_layer_bias_initializer=self._model_config_dict[
                "head_fcnet_bias_initializer"
            ],
            output_layer_bias_initializer_config=self._model_config_dict[
                "head_fcnet_bias_initializer_kwargs"
            ],
        )
