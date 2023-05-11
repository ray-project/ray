import enum
import functools
from typing import Optional

import gymnasium as gym
import numpy as np
import tree
from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple

from ray.rllib.core.models.base import Encoder
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.core.models.configs import (
    CNNEncoderConfig,
    MLPEncoderConfig,
    RecurrentEncoderConfig,
)
from ray.rllib.models.preprocessors import get_preprocessor, Preprocessor
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.models.distributions import Distribution
from ray.rllib.models.utils import get_filter_config
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.spaces.space_utils import flatten_space
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space


def _multi_action_dist_partial_helper(
    catalog_cls: "Catalog", action_space: gym.Space, framework: str
) -> Distribution:
    """Helper method to get a partial of a MultiActionDistribution.

    This is useful for when we want to create MultiActionDistributions from
    logits only (!) later, but know the action space now already.

    Args:
        catalog_cls: The ModelCatalog class to use.
        action_space: The action space to get the child distribution classes for.
        framework: The framework to use.

    Returns:
        A partial of the TorchMultiActionDistribution class.
    """
    action_space_struct = get_base_struct_from_space(action_space)
    flat_action_space = flatten_space(action_space)
    child_distribution_cls_struct = tree.map_structure(
        lambda s: catalog_cls.get_dist_cls_from_action_space(
            action_space=s,
            framework=framework,
        ),
        action_space_struct,
    )
    flat_distribution_clses = tree.flatten(child_distribution_cls_struct)

    logit_lens = [
        int(dist_cls.required_input_dim(space))
        for dist_cls, space in zip(flat_distribution_clses, flat_action_space)
    ]

    if framework == "torch":
        from ray.rllib.models.torch.torch_distributions import (
            TorchMultiDistribution,
        )

        multi_action_dist_cls = TorchMultiDistribution
    elif framework == "tf2":
        from ray.rllib.models.tf.tf_distributions import TfMultiDistribution

        multi_action_dist_cls = TfMultiDistribution
    else:
        raise ValueError(f"Unsupported framework: {framework}")

    partial_dist_cls = multi_action_dist_cls.get_partial_dist_cls(
        space=action_space,
        child_distribution_cls_struct=child_distribution_cls_struct,
        input_lens=logit_lens,
    )
    return partial_dist_cls


def _multi_categorical_dist_partial_helper(
    action_space: gym.Space, framework: str
) -> Distribution:
    """Helper method to get a partial of a MultiCategorical Distribution.

    This is useful for when we want to create MultiCategorical Distribution from
    logits only (!) later, but know the action space now already.

    Args:
        action_space: The action space to get the child distribution classes for.
        framework: The framework to use.

    Returns:
        A partial of the MultiCategorical class.
    """

    if framework == "torch":
        from ray.rllib.models.torch.torch_distributions import TorchMultiCategorical

        multi_categorical_dist_cls = TorchMultiCategorical
    elif framework == "tf2":
        from ray.rllib.models.tf.tf_distributions import TfMultiCategorical

        multi_categorical_dist_cls = TfMultiCategorical
    else:
        raise ValueError(f"Unsupported framework: {framework}")

    partial_dist_cls = multi_categorical_dist_cls.get_partial_dist_cls(
        space=action_space, input_lens=list(action_space.nvec)
    )

    return partial_dist_cls


class Catalog:
    """Describes the sub-modules architectures to be used in RLModules.

    RLlib's native RLModules get their Models from a Catalog object.
    By default, that Catalog builds the configs it has as attributes.
    You can modify a Catalog so that it builds different Models by subclassing and
    overriding the build_* methods. Alternatively, you can customize the configs
    inside RLlib's Catalogs to customize what is being built by RLlib.

    Usage example:

    # Define a custom catalog

    .. testcode::

        import torch
        import gymnasium as gym
        from ray.rllib.core.models.configs import MLPHeadConfig
        from ray.rllib.core.models.catalog import Catalog


        class MyCatalog(Catalog):
            def __init__(
                self,
                observation_space: gym.Space,
                action_space: gym.Space,
                model_config_dict: dict,
            ):
                super().__init__(observation_space, action_space, model_config_dict)
                self.my_model_config_dict = MLPHeadConfig(
                    hidden_layer_dims=[64, 32],
                    input_dims=[self.observation_space.shape[0]],
                    output_dims=[1],
                )

            def build_my_head(self, framework: str):
                return self.my_model_config_dict.build(framework=framework)

        # With that, RLlib can build and use models from this catalog like this:
        catalog = MyCatalog(gym.spaces.Box(0, 1), gym.spaces.Box(0, 1), {})
        my_head = catalog.build_my_head("torch")
        out = my_head(torch.Tensor([[1]]))
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        # TODO (Artur): Turn model_config into model_config_dict to distinguish
        #  between ModelConfig and a model_config_dict dict library-wide.
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        """Initializes a Catalog with a default encoder config.

        Args:
            observation_space: The observation space of the environment.
            action_space: The action space of the environment.
            model_config_dict: The model config that specifies things like hidden
                dimensions and activations functions to use in this Catalog.
            view_requirements: The view requirements of Models to produce. This is
                needed for a Model that encodes a complex temporal mix of
                observations, actions or rewards.

        """
        self.observation_space = observation_space
        self.action_space = action_space

        # TODO (Artur): Make model defaults a dataclass
        self.model_config_dict = {**MODEL_DEFAULTS, **model_config_dict}
        self.view_requirements = view_requirements

        self._latent_dims = None

        # Overwrite this post-init hook in subclasses
        self.__post_init__()

    @property
    def latent_dims(self):
        """Returns the latent dimensions of the encoder.

        This establishes an agreement between encoder and heads about the latent
        dimensions. Encoders can be built to output a latent tensor with
        `latent_dims` dimensions, and heads can be built with tensors of
        `latent_dims` dimensions as inputs. This can be safely ignored if this
        agreement is not needed in case of modifications to the Catalog.

        Returns:
            The latent dimensions of the encoder.
        """
        return self._latent_dims

    @latent_dims.setter
    def latent_dims(self, value):
        self._latent_dims = value

    def __post_init__(self):
        """Post-init hook for subclasses to override.

        This makes it so that subclasses are not forced to create an encoder config
        if the rest of their catalog is not dependent on it or if it breaks.
        At the end of Catalog initialization, an attribute `Catalog.latent_dims`
        should be set so that heads can be built using that information.
        """
        self.encoder_config = self.get_encoder_config(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config_dict=self.model_config_dict,
            view_requirements=self.view_requirements,
        )

        # Create a function that can be called when framework is known to retrieve the
        # class type for action distributions
        self._action_dist_class_fn = functools.partial(
            self.get_dist_cls_from_action_space, action_space=self.action_space
        )

        # The dimensions of the latent vector that is output by the encoder and fed
        # to the heads.
        self.latent_dims = self.encoder_config.output_dims

    def build_encoder(self, framework: str) -> Encoder:
        """Builds the encoder.

        By default this method builds an encoder instance from Catalog.encoder_config.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The encoder.
        """
        assert hasattr(self, "encoder_config"), (
            "You must define a `Catalog.encoder_config` attribute in your Catalog "
            "subclass or override the `Catalog.build_encoder` method. By default, "
            "an encoder_config is created in the __post_init__ method."
        )
        return self.encoder_config.build(framework=framework)

    def get_action_dist_cls(self, framework: str):
        """Get the action distribution class.

        The default behavior is to get the action distribution from the
        `Catalog.action_dist_class_fn`. This can be overridden to build a custom action
        distribution as a means of configuring the behavior of a RLModule
        implementation.

        Args:
            framework: The framework to use. Either "torch" or "tf2".

        Returns:
            The action distribution.
        """
        assert hasattr(self, "_action_dist_class_fn"), (
            "You must define a `Catalog._action_dist_class_fn` attribute in your "
            "Catalog subclass or override the `Catalog.action_dist_class_fn` method. "
            "By default, an action_dist_class_fn is created in the __post_init__ "
            "method."
        )
        return self._action_dist_class_fn(framework=framework)

    @classmethod
    def get_encoder_config(
        cls,
        observation_space: gym.Space,
        model_config_dict: dict,
        action_space: gym.Space = None,
        view_requirements=None,
    ) -> ModelConfig:
        """Returns an EncoderConfig for the given input_space and model_config_dict.

        Encoders are usually used in RLModules to transform the input space into a
        latent space that is then fed to the heads. The returned EncoderConfig
        objects correspond to the built-in Encoder classes in RLlib.
        For example, for a simple 1D-Box input_space, RLlib offers an
        MLPEncoder, hence this method returns the MLPEncoderConfig. You can overwrite
        this method to produce specific EncoderConfigs for your custom Models.

        The following input spaces lead to the following configs:
        - 1D-Box: MLPEncoderConfig
        - 3D-Box: CNNEncoderConfig
        # TODO (Artur): Support more spaces here
        # ...

        Args:
            observation_space: The observation space to use.
            model_config_dict: The model config to use.
            action_space: The action space to use if actions are to be encoded. This
                is commonly the case for LSTM models.
            view_requirements: The view requirements to use if anything else than
                observation_space or action_space is to be encoded. This signifies an
                advanced use case.

        Returns:
            The encoder config.
        """
        # TODO (Artur): Make it so that we don't work with complete MODEL_DEFAULTS
        model_config_dict = {**MODEL_DEFAULTS, **model_config_dict}

        activation = model_config_dict["fcnet_activation"]
        output_activation = model_config_dict["fcnet_activation"]
        fcnet_hiddens = model_config_dict["fcnet_hiddens"]
        # TODO (sven): Move to a new ModelConfig object (dataclass) asap, instead of
        #  "linking" into the old ModelConfig (dict)! This just causes confusion as to
        #  which old keys now mean what for the new RLModules-based default models.
        encoder_latent_dim = (
            model_config_dict["encoder_latent_dim"] or fcnet_hiddens[-1]
        )
        use_lstm = model_config_dict["use_lstm"]
        use_attention = model_config_dict["use_attention"]

        if use_lstm:
            encoder_config = RecurrentEncoderConfig(
                recurrent_layer_type="lstm",
                hidden_dim=model_config_dict["lstm_cell_size"],
                batch_major=not model_config_dict["_time_major"],
                num_layers=1,
                output_dims=[model_config_dict["lstm_cell_size"]],
                output_activation=output_activation,
                view_requirements_dict=view_requirements,
                get_tokenizer_config=cls.get_tokenizer_config,
            )
        elif use_attention:
            raise NotImplementedError
        else:
            # TODO (Artur): Maybe check for original spaces here
            # input_space is a 1D Box
            if isinstance(observation_space, Box) and len(observation_space.shape) == 1:
                # In order to guarantee backward compatability with old configs,
                # we need to check if no latent dim was set and simply reuse the last
                # fcnet hidden dim for that purpose.
                if model_config_dict["encoder_latent_dim"]:
                    hidden_layer_dims = model_config_dict["fcnet_hiddens"]
                else:
                    hidden_layer_dims = model_config_dict["fcnet_hiddens"][:-1]
                encoder_config = MLPEncoderConfig(
                    input_dims=[observation_space.shape[0]],
                    hidden_layer_dims=hidden_layer_dims,
                    hidden_layer_activation=activation,
                    output_dims=[encoder_latent_dim],
                    output_activation=output_activation,
                )

            # input_space is a 3D Box
            elif (
                isinstance(observation_space, Box) and len(observation_space.shape) == 3
            ):
                if not model_config_dict.get("conv_filters"):
                    model_config_dict["conv_filters"] = get_filter_config(
                        observation_space.shape
                    )

                encoder_config = CNNEncoderConfig(
                    input_dims=observation_space.shape,
                    cnn_filter_specifiers=model_config_dict["conv_filters"],
                    cnn_activation=model_config_dict["conv_activation"],
                    cnn_use_layernorm=model_config_dict.get(
                        "conv_use_layernorm", False
                    ),
                    output_dims=[encoder_latent_dim],
                    # TODO (sven): Setting this to None here helps with the existing
                    #  APPO Pong benchmark (actually, leaving this at default=tanh does
                    #  NOT learn at all!).
                    #  We need to remove the last Dense layer from CNNEncoder in general
                    #  AND establish proper ModelConfig objects (instead of hacking
                    #  everything with the old default model config dict).
                    output_activation=None,
                )
            # input_space is a 2D Box
            elif (
                isinstance(observation_space, Box) and len(observation_space.shape) == 2
            ):
                # RLlib used to support 2D Box spaces by silently flattening them
                raise ValueError(
                    f"No default encoder config for obs space={observation_space},"
                    f" lstm={use_lstm} and attention={use_attention} found. 2D Box "
                    f"spaces are not supported. They should be either flattened to a "
                    f"1D Box space or enhanced to be a 3D box space."
                )
            # input_space is a possibly nested structure of spaces.
            else:
                # NestedModelConfig
                raise ValueError(
                    f"No default encoder config for obs space={observation_space},"
                    f" lstm={use_lstm} and attention={use_attention} found."
                )

        return encoder_config

    @classmethod
    def get_tokenizer_config(
        cls, space: gym.Space, model_config_dict: dict
    ) -> ModelConfig:
        """Returns a tokenizer config for the given space.

        This is useful for LSTM models that need to tokenize their inputs.
        By default, RLlib uses the models supported by Catalog out of the box to
        tokenize.
        """
        return cls.get_encoder_config(
            observation_space=space,
            # Use model_config_dict without flags that would end up in complex models
            model_config_dict={
                **model_config_dict,
                **{"use_lstm": False, "use_attention": False},
            },
        )

    @classmethod
    def get_dist_cls_from_action_space(
        cls,
        action_space: gym.Space,
        *,
        framework: Optional[str] = None,
    ) -> Distribution:
        """Returns a distribution class for the given action space.

        You can get the required input dimension for the distribution by calling
        `action_dict_cls.required_input_dim(action_space)`
        on the retrieved class. This is useful, because the Catalog needs to find out
        about the required input dimension for the distribution before the model that
        outputs these inputs is configured.

        Args:
            action_space: Action space of the target gym env.
            framework: The framework to use.

        Returns:
            The distribution class for the given action space.
        """
        # This method is structured in two steps:
        # Firstly, construct a dictionary containing the available distribution classes.
        # Secondly, return the correct distribution class for the given action space.

        # Step 1: Construct the dictionary.

        class DistEnum(enum.Enum):
            Categorical = "Categorical"
            DiagGaussian = "Gaussian"
            Deterministic = "Deterministic"
            MultiDistribution = "MultiDistribution"
            MultiCategorical = "MultiCategorical"

        if framework == "torch":
            from ray.rllib.models.torch.torch_distributions import (
                TorchCategorical,
                TorchDeterministic,
                TorchDiagGaussian,
            )

            distribution_dicts = {
                DistEnum.Deterministic: TorchDeterministic,
                DistEnum.DiagGaussian: TorchDiagGaussian,
                DistEnum.Categorical: TorchCategorical,
            }
        elif framework == "tf2":
            from ray.rllib.models.tf.tf_distributions import (
                TfCategorical,
                TfDeterministic,
                TfDiagGaussian,
            )

            distribution_dicts = {
                DistEnum.Deterministic: TfDeterministic,
                DistEnum.DiagGaussian: TfDiagGaussian,
                DistEnum.Categorical: TfCategorical,
            }
        else:
            raise ValueError(
                f"Unknown framework: {framework}. Only 'torch' and 'tf2' are "
                "supported for RLModule Catalogs."
            )

        # Only add a MultiAction distribution class to the dict if we can compute its
        # components (we need a Tuple/Dict space for this).
        if isinstance(action_space, (Tuple, Dict)):
            partial_multi_action_distribution_cls = _multi_action_dist_partial_helper(
                catalog_cls=cls,
                action_space=action_space,
                framework=framework,
            )

            distribution_dicts[
                DistEnum.MultiDistribution
            ] = partial_multi_action_distribution_cls

        # Only add a MultiCategorical distribution class to the dict if we can compute
        # its components (we need a MultiDiscrete space for this).
        if isinstance(action_space, MultiDiscrete):
            partial_multi_categorical_distribution_cls = (
                _multi_categorical_dist_partial_helper(
                    action_space=action_space,
                    framework=framework,
                )
            )

            distribution_dicts[
                DistEnum.MultiCategorical
            ] = partial_multi_categorical_distribution_cls

        # Step 2: Return the correct distribution class for the given action space.

        # Box space -> DiagGaussian OR Deterministic.
        if isinstance(action_space, Box):
            if action_space.dtype.char in np.typecodes["AllInteger"]:
                raise ValueError(
                    "Box(..., `int`) action spaces are not supported. "
                    "Use MultiDiscrete  or Box(..., `float`)."
                )
            else:
                if len(action_space.shape) > 1:
                    raise UnsupportedSpaceException(
                        f"Action space has multiple dimensions {action_space.shape}. "
                        f"Consider reshaping this into a single dimension, using a "
                        f"custom action distribution, using a Tuple action space, "
                        f"or the multi-agent API."
                    )
                return distribution_dicts[DistEnum.DiagGaussian]

        # Discrete Space -> Categorical.
        elif isinstance(action_space, Discrete):
            return distribution_dicts[DistEnum.Categorical]

        # Tuple/Dict Spaces -> MultiAction.
        elif isinstance(action_space, (Tuple, Dict)):
            return distribution_dicts[DistEnum.MultiDistribution]

        # Simplex -> Dirichlet.
        elif isinstance(action_space, Simplex):
            # TODO(Artur): Supported Simplex (in torch).
            raise NotImplementedError("Simplex action space not yet supported.")

        # MultiDiscrete -> MultiCategorical.
        elif isinstance(action_space, MultiDiscrete):
            return distribution_dicts[DistEnum.MultiCategorical]

        # Unknown type -> Error.
        else:
            raise NotImplementedError(f"Unsupported action space: `{action_space}`")

    @staticmethod
    def get_preprocessor(observation_space: gym.Space, **kwargs) -> Preprocessor:
        """Returns a suitable preprocessor for the given observation space.

        Args:
            observation_space: The input observation space.
            **kwargs: Forward-compatible kwargs.

        Returns:
            preprocessor: Preprocessor for the observations.
        """
        # TODO(Artur): Since preprocessors have long been @PublicAPI with the options
        #  kwarg as part of their constructor, we fade out support for this,
        #  beginning with this entrypoint.
        # Next, we should deprecate the `options` kwarg from the Preprocessor itself,
        # after deprecating the old catalog and other components that still pass this.
        options = kwargs.get("options", {})
        if options:
            deprecation_warning(
                old="get_preprocessor_for_space(..., options={...})",
                help="Override `Catalog.get_preprocessor()` "
                "in order to implement custom behaviour.",
                error=False,
            )

        if options.get("custom_preprocessor"):
            deprecation_warning(
                old="model_config['custom_preprocessor']",
                help="Custom preprocessors are deprecated, "
                "since they sometimes conflict with the built-in "
                "preprocessors for handling complex observation spaces. "
                "Please use wrapper classes around your environment "
                "instead.",
                error=True,
            )
        else:
            # TODO(Artur): Inline the get_preprocessor() call here once we have
            #  deprecated the old model catalog.
            cls = get_preprocessor(observation_space)
            prep = cls(observation_space, options)
            return prep
