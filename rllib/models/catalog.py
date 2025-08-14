from functools import partial
import gymnasium as gym
from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple
import logging
import numpy as np
import tree  # pip install dm_tree
from typing import List, Optional, Type, Union

from ray.tune.registry import (
    RLLIB_MODEL,
    RLLIB_ACTION_DIST,
    _global_registry,
)
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.preprocessors import get_preprocessor, Preprocessor
from ray.rllib.models.tf.tf_action_dist import (
    Categorical,
    Deterministic,
    DiagGaussian,
    Dirichlet,
    MultiActionDistribution,
    MultiCategorical,
)
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDeterministic,
    TorchDirichlet,
    TorchDiagGaussian,
    TorchMultiActionDistribution,
    TorchMultiCategorical,
)
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.spaces.space_utils import flatten_space
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
MODEL_DEFAULTS: ModelConfigDict = {
    "fcnet_hiddens": [256, 256],
    "fcnet_activation": "tanh",
    "fcnet_weights_initializer": None,
    "fcnet_weights_initializer_config": None,
    "fcnet_bias_initializer": None,
    "fcnet_bias_initializer_config": None,
    "conv_filters": None,
    "conv_activation": "relu",
    "conv_kernel_initializer": None,
    "conv_kernel_initializer_config": None,
    "conv_bias_initializer": None,
    "conv_bias_initializer_config": None,
    "conv_transpose_kernel_initializer": None,
    "conv_transpose_kernel_initializer_config": None,
    "conv_transpose_bias_initializer": None,
    "conv_transpose_bias_initializer_config": None,
    "post_fcnet_hiddens": [],
    "post_fcnet_activation": "relu",
    "post_fcnet_weights_initializer": None,
    "post_fcnet_weights_initializer_config": None,
    "post_fcnet_bias_initializer": None,
    "post_fcnet_bias_initializer_config": None,
    "free_log_std": False,
    "log_std_clip_param": 20.0,
    "no_final_linear": False,
    "vf_share_layers": True,
    "use_lstm": False,
    "max_seq_len": 20,
    "lstm_cell_size": 256,
    "lstm_use_prev_action": False,
    "lstm_use_prev_reward": False,
    "lstm_weights_initializer": None,
    "lstm_weights_initializer_config": None,
    "lstm_bias_initializer": None,
    "lstm_bias_initializer_config": None,
    "_time_major": False,
    "use_attention": False,
    "attention_num_transformer_units": 1,
    "attention_dim": 64,
    "attention_num_heads": 1,
    "attention_head_dim": 32,
    "attention_memory_inference": 50,
    "attention_memory_training": 50,
    "attention_position_wise_mlp_dim": 32,
    "attention_init_gru_gate_bias": 2.0,
    "attention_use_n_prev_actions": 0,
    "attention_use_n_prev_rewards": 0,
    "framestack": True,
    "dim": 84,
    "grayscale": False,
    "zero_mean": True,
    "custom_model": None,
    "custom_model_config": {},
    "custom_action_dist": None,
    "custom_preprocessor": None,
    "encoder_latent_dim": None,
    "always_check_shapes": False,

    # Deprecated keys:
    "lstm_use_prev_action_reward": DEPRECATED_VALUE,
    "_use_default_native_models": DEPRECATED_VALUE,
    "_disable_preprocessor_api": False,
    "_disable_action_flattening": False,
}
# __sphinx_doc_end__
# fmt: on


@DeveloperAPI
class ModelCatalog:
    """Registry of models, preprocessors, and action distributions for envs.

    .. testcode::
        :skipif: True

        prep = ModelCatalog.get_preprocessor(env)
        observation = prep.transform(raw_observation)

        dist_class, dist_dim = ModelCatalog.get_action_dist(
            env.action_space, {})
        model = ModelCatalog.get_model_v2(
            obs_space, action_space, num_outputs, options)
        dist = dist_class(model.outputs, model)
        action = dist.sample()
    """

    @staticmethod
    @DeveloperAPI
    def get_action_dist(
        action_space: gym.Space,
        config: ModelConfigDict,
        dist_type: Optional[Union[str, Type[ActionDistribution]]] = None,
        framework: str = "tf",
        **kwargs
    ) -> (type, int):
        """Returns a distribution class and size for the given action space.

        Args:
            action_space: Action space of the target gym env.
            config (Optional[dict]): Optional model config.
            dist_type (Optional[Union[str, Type[ActionDistribution]]]):
                Identifier of the action distribution (str) interpreted as a
                hint or the actual ActionDistribution class to use.
            framework: One of "tf2", "tf", "torch", or "jax".
            kwargs: Optional kwargs to pass on to the Distribution's
                constructor.

        Returns:
            Tuple:
                - dist_class (ActionDistribution): Python class of the
                    distribution.
                - dist_dim (int): The size of the input vector to the
                    distribution.
        """

        dist_cls = None
        config = config or MODEL_DEFAULTS
        # Custom distribution given.
        if config.get("custom_action_dist"):
            custom_action_config = config.copy()
            action_dist_name = custom_action_config.pop("custom_action_dist")
            logger.debug("Using custom action distribution {}".format(action_dist_name))
            dist_cls = _global_registry.get(RLLIB_ACTION_DIST, action_dist_name)
            return ModelCatalog._get_multi_action_distribution(
                dist_cls, action_space, custom_action_config, framework
            )

        # Dist_type is given directly as a class.
        elif (
            type(dist_type) is type
            and issubclass(dist_type, ActionDistribution)
            and dist_type not in (MultiActionDistribution, TorchMultiActionDistribution)
        ):
            dist_cls = dist_type
        # Box space -> DiagGaussian OR Deterministic.
        elif isinstance(action_space, Box):
            if action_space.dtype.name.startswith("int"):
                low_ = np.min(action_space.low)
                high_ = np.max(action_space.high)
                dist_cls = (
                    TorchMultiCategorical if framework == "torch" else MultiCategorical
                )
                num_cats = int(np.prod(action_space.shape))
                return (
                    partial(
                        dist_cls,
                        input_lens=[high_ - low_ + 1 for _ in range(num_cats)],
                        action_space=action_space,
                    ),
                    num_cats * (high_ - low_ + 1),
                )
            else:
                if len(action_space.shape) > 1:
                    raise UnsupportedSpaceException(
                        "Action space has multiple dimensions "
                        "{}. ".format(action_space.shape)
                        + "Consider reshaping this into a single dimension, "
                        "using a custom action distribution, "
                        "using a Tuple action space, or the multi-agent API."
                    )
                # TODO(sven): Check for bounds and return SquashedNormal, etc..
                if dist_type is None:
                    return (
                        partial(
                            TorchDiagGaussian if framework == "torch" else DiagGaussian,
                            action_space=action_space,
                        ),
                        DiagGaussian.required_model_output_shape(action_space, config),
                    )
                elif dist_type == "deterministic":
                    dist_cls = (
                        TorchDeterministic if framework == "torch" else Deterministic
                    )
        # Discrete Space -> Categorical.
        elif isinstance(action_space, Discrete):
            if framework == "torch":
                dist_cls = TorchCategorical
            elif framework == "jax":
                from ray.rllib.models.jax.jax_action_dist import JAXCategorical

                dist_cls = JAXCategorical
            else:
                dist_cls = Categorical
        # Tuple/Dict Spaces -> MultiAction.
        elif dist_type in (
            MultiActionDistribution,
            TorchMultiActionDistribution,
        ) or isinstance(action_space, (Tuple, Dict)):
            return ModelCatalog._get_multi_action_distribution(
                (
                    MultiActionDistribution
                    if framework == "tf"
                    else TorchMultiActionDistribution
                ),
                action_space,
                config,
                framework,
            )
        # Simplex -> Dirichlet.
        elif isinstance(action_space, Simplex):
            dist_cls = TorchDirichlet if framework == "torch" else Dirichlet
        # MultiDiscrete -> MultiCategorical.
        elif isinstance(action_space, MultiDiscrete):
            dist_cls = (
                TorchMultiCategorical if framework == "torch" else MultiCategorical
            )
            return partial(dist_cls, input_lens=action_space.nvec), int(
                sum(action_space.nvec)
            )
        # Unknown type -> Error.
        else:
            raise NotImplementedError(
                "Unsupported args: {} {}".format(action_space, dist_type)
            )

        return dist_cls, int(dist_cls.required_model_output_shape(action_space, config))

    @staticmethod
    @DeveloperAPI
    def get_action_shape(
        action_space: gym.Space, framework: str = "tf"
    ) -> (np.dtype, List[int]):
        """Returns action tensor dtype and shape for the action space.

        Args:
            action_space: Action space of the target gym env.
            framework: The framework identifier. One of "tf" or "torch".

        Returns:
            (dtype, shape): Dtype and shape of the actions tensor.
        """
        dl_lib = torch if framework == "torch" else tf
        if isinstance(action_space, Discrete):
            return action_space.dtype, (None,)
        elif isinstance(action_space, (Box, Simplex)):
            if np.issubdtype(action_space.dtype, np.floating):
                return dl_lib.float32, (None,) + action_space.shape
            elif np.issubdtype(action_space.dtype, np.integer):
                return dl_lib.int32, (None,) + action_space.shape
            else:
                raise ValueError("RLlib doesn't support non int or float box spaces")
        elif isinstance(action_space, MultiDiscrete):
            return action_space.dtype, (None,) + action_space.shape
        elif isinstance(action_space, (Tuple, Dict)):
            flat_action_space = flatten_space(action_space)
            size = 0
            all_discrete = True
            for i in range(len(flat_action_space)):
                if isinstance(flat_action_space[i], Discrete):
                    size += 1
                else:
                    all_discrete = False
                    size += np.prod(flat_action_space[i].shape)
            size = int(size)
            return dl_lib.int32 if all_discrete else dl_lib.float32, (None, size)
        else:
            raise NotImplementedError(
                "Action space {} not supported".format(action_space)
            )

    @staticmethod
    @DeveloperAPI
    def get_action_placeholder(
        action_space: gym.Space, name: str = "action"
    ) -> TensorType:
        """Returns an action placeholder consistent with the action space

        Args:
            action_space: Action space of the target gym env.
            name: An optional string to name the placeholder by.
                Default: "action".

        Returns:
            action_placeholder: A placeholder for the actions
        """
        dtype, shape = ModelCatalog.get_action_shape(action_space, framework="tf")

        return tf1.placeholder(dtype, shape=shape, name=name)

    @staticmethod
    @DeveloperAPI
    def get_model_v2(
        obs_space: gym.Space,
        action_space: gym.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        framework: str = "tf",
        name: str = "default_model",
        model_interface: type = None,
        default_model: type = None,
        **model_kwargs
    ) -> ModelV2:
        """Returns a suitable model compatible with given spaces and output.

        Args:
            obs_space: Observation space of the target gym env. This
                may have an `original_space` attribute that specifies how to
                unflatten the tensor into a ragged tensor.
            action_space: Action space of the target gym env.
            num_outputs: The size of the output vector of the model.
            model_config: The "model" sub-config dict
                within the Algorithm's config dict.
            framework: One of "tf2", "tf", "torch", or "jax".
            name: Name (scope) for the model.
            model_interface: Interface required for the model
            default_model: Override the default class for the model. This
                only has an effect when not using a custom model
            model_kwargs: Args to pass to the ModelV2 constructor

        Returns:
            model (ModelV2): Model to use for the policy.
        """

        # Validate the given config dict.
        ModelCatalog._validate_config(
            config=model_config, action_space=action_space, framework=framework
        )

        if model_config.get("custom_model"):
            # Allow model kwargs to be overridden / augmented by
            # custom_model_config.
            customized_model_kwargs = dict(
                model_kwargs, **model_config.get("custom_model_config", {})
            )

            if isinstance(model_config["custom_model"], type):
                model_cls = model_config["custom_model"]
            elif (
                isinstance(model_config["custom_model"], str)
                and "." in model_config["custom_model"]
            ):
                return from_config(
                    cls=model_config["custom_model"],
                    obs_space=obs_space,
                    action_space=action_space,
                    num_outputs=num_outputs,
                    model_config=customized_model_kwargs,
                    name=name,
                )
            else:
                model_cls = _global_registry.get(
                    RLLIB_MODEL, model_config["custom_model"]
                )

            # Only allow ModelV2 or native keras Models.
            if not issubclass(model_cls, ModelV2):
                if framework not in ["tf", "tf2"] or not issubclass(
                    model_cls, tf.keras.Model
                ):
                    raise ValueError(
                        "`model_cls` must be a ModelV2 sub-class, but is"
                        " {}!".format(model_cls)
                    )

            logger.info("Wrapping {} as {}".format(model_cls, model_interface))
            model_cls = ModelCatalog._wrap_if_needed(model_cls, model_interface)

            if framework in ["tf2", "tf"]:
                # Try wrapping custom model with LSTM/attention, if required.
                if model_config.get("use_lstm") or model_config.get("use_attention"):
                    from ray.rllib.models.tf.attention_net import (
                        AttentionWrapper,
                    )
                    from ray.rllib.models.tf.recurrent_net import (
                        LSTMWrapper,
                    )

                    wrapped_cls = model_cls
                    forward = wrapped_cls.forward
                    model_cls = ModelCatalog._wrap_if_needed(
                        wrapped_cls,
                        LSTMWrapper
                        if model_config.get("use_lstm")
                        else AttentionWrapper,
                    )
                    model_cls._wrapped_forward = forward

                # Obsolete: Track and warn if vars were created but not
                # registered. Only still do this, if users do register their
                # variables. If not (which they shouldn't), don't check here.
                created = set()

                def track_var_creation(next_creator, **kw):
                    v = next_creator(**kw)
                    created.add(v.ref())
                    return v

                with tf.variable_creator_scope(track_var_creation):
                    if issubclass(model_cls, tf.keras.Model):
                        instance = model_cls(
                            input_space=obs_space,
                            action_space=action_space,
                            num_outputs=num_outputs,
                            name=name,
                            **customized_model_kwargs,
                        )
                    else:
                        # Try calling with kwargs first (custom ModelV2 should
                        # accept these as kwargs, not get them from
                        # config["custom_model_config"] anymore).
                        try:
                            instance = model_cls(
                                obs_space,
                                action_space,
                                num_outputs,
                                model_config,
                                name,
                                **customized_model_kwargs,
                            )
                        except TypeError as e:
                            # Keyword error: Try old way w/o kwargs.
                            if "__init__() got an unexpected " in e.args[0]:
                                instance = model_cls(
                                    obs_space,
                                    action_space,
                                    num_outputs,
                                    model_config,
                                    name,
                                    **model_kwargs,
                                )
                                logger.warning(
                                    "Custom ModelV2 should accept all custom "
                                    "options as **kwargs, instead of expecting"
                                    " them in config['custom_model_config']!"
                                )
                            # Other error -> re-raise.
                            else:
                                raise e

                # User still registered TFModelV2's variables: Check, whether
                # ok.
                registered = []
                if not isinstance(instance, tf.keras.Model):
                    registered = set(instance.var_list)
                if len(registered) > 0:
                    not_registered = set()
                    for var in created:
                        if var not in registered:
                            not_registered.add(var)
                    if not_registered:
                        raise ValueError(
                            "It looks like you are still using "
                            "`{}.register_variables()` to register your "
                            "model's weights. This is no longer required, but "
                            "if you are still calling this method at least "
                            "once, you must make sure to register all created "
                            "variables properly. The missing variables are {},"
                            " and you only registered {}. "
                            "Did you forget to call `register_variables()` on "
                            "some of the variables in question?".format(
                                instance, not_registered, registered
                            )
                        )
            elif framework == "torch":
                # Try wrapping custom model with LSTM/attention, if required.
                if model_config.get("use_lstm") or model_config.get("use_attention"):
                    from ray.rllib.models.torch.attention_net import AttentionWrapper
                    from ray.rllib.models.torch.recurrent_net import LSTMWrapper

                    wrapped_cls = model_cls
                    forward = wrapped_cls.forward
                    model_cls = ModelCatalog._wrap_if_needed(
                        wrapped_cls,
                        LSTMWrapper
                        if model_config.get("use_lstm")
                        else AttentionWrapper,
                    )
                    model_cls._wrapped_forward = forward

                # PyTorch automatically tracks nn.Modules inside the parent
                # nn.Module's constructor.
                # Try calling with kwargs first (custom ModelV2 should
                # accept these as kwargs, not get them from
                # config["custom_model_config"] anymore).
                try:
                    instance = model_cls(
                        obs_space,
                        action_space,
                        num_outputs,
                        model_config,
                        name,
                        **customized_model_kwargs,
                    )
                except TypeError as e:
                    # Keyword error: Try old way w/o kwargs.
                    if "__init__() got an unexpected " in e.args[0]:
                        instance = model_cls(
                            obs_space,
                            action_space,
                            num_outputs,
                            model_config,
                            name,
                            **model_kwargs,
                        )
                        logger.warning(
                            "Custom ModelV2 should accept all custom "
                            "options as **kwargs, instead of expecting"
                            " them in config['custom_model_config']!"
                        )
                    # Other error -> re-raise.
                    else:
                        raise e
            else:
                raise NotImplementedError(
                    "`framework` must be 'tf2|tf|torch', but is "
                    "{}!".format(framework)
                )

            return instance

        # Find a default TFModelV2 and wrap with model_interface.
        if framework in ["tf", "tf2"]:
            v2_class = None
            # Try to get a default v2 model.
            if not model_config.get("custom_model"):
                v2_class = default_model or ModelCatalog._get_v2_model_class(
                    obs_space, model_config, framework=framework
                )

            if not v2_class:
                raise ValueError("ModelV2 class could not be determined!")

            if model_config.get("use_lstm") or model_config.get("use_attention"):
                from ray.rllib.models.tf.attention_net import (
                    AttentionWrapper,
                )
                from ray.rllib.models.tf.recurrent_net import (
                    LSTMWrapper,
                )

                wrapped_cls = v2_class
                if model_config.get("use_lstm"):
                    v2_class = ModelCatalog._wrap_if_needed(wrapped_cls, LSTMWrapper)
                    v2_class._wrapped_forward = wrapped_cls.forward
                else:
                    v2_class = ModelCatalog._wrap_if_needed(
                        wrapped_cls, AttentionWrapper
                    )
                    v2_class._wrapped_forward = wrapped_cls.forward

            # Wrap in the requested interface.
            wrapper = ModelCatalog._wrap_if_needed(v2_class, model_interface)

            if issubclass(wrapper, tf.keras.Model):
                model = wrapper(
                    input_space=obs_space,
                    action_space=action_space,
                    num_outputs=num_outputs,
                    name=name,
                    **dict(model_kwargs, **model_config),
                )
                return model

            return wrapper(
                obs_space, action_space, num_outputs, model_config, name, **model_kwargs
            )

        # Find a default TorchModelV2 and wrap with model_interface.
        elif framework == "torch":
            # Try to get a default v2 model.
            if not model_config.get("custom_model"):
                v2_class = default_model or ModelCatalog._get_v2_model_class(
                    obs_space, model_config, framework=framework
                )

            if not v2_class:
                raise ValueError("ModelV2 class could not be determined!")

            if model_config.get("use_lstm") or model_config.get("use_attention"):
                from ray.rllib.models.torch.attention_net import AttentionWrapper
                from ray.rllib.models.torch.recurrent_net import LSTMWrapper

                wrapped_cls = v2_class
                forward = wrapped_cls.forward
                if model_config.get("use_lstm"):
                    v2_class = ModelCatalog._wrap_if_needed(wrapped_cls, LSTMWrapper)
                else:
                    v2_class = ModelCatalog._wrap_if_needed(
                        wrapped_cls, AttentionWrapper
                    )

                v2_class._wrapped_forward = forward

            # Wrap in the requested interface.
            wrapper = ModelCatalog._wrap_if_needed(v2_class, model_interface)
            return wrapper(
                obs_space, action_space, num_outputs, model_config, name, **model_kwargs
            )

        # Find a default JAXModelV2 and wrap with model_interface.
        elif framework == "jax":
            v2_class = default_model or ModelCatalog._get_v2_model_class(
                obs_space, model_config, framework=framework
            )
            # Wrap in the requested interface.
            wrapper = ModelCatalog._wrap_if_needed(v2_class, model_interface)
            return wrapper(
                obs_space, action_space, num_outputs, model_config, name, **model_kwargs
            )
        else:
            raise NotImplementedError(
                "`framework` must be 'tf2|tf|torch', but is {}!".format(framework)
            )

    @staticmethod
    @DeveloperAPI
    def get_preprocessor(
        env: gym.Env, options: Optional[dict] = None, include_multi_binary: bool = False
    ) -> Preprocessor:
        """Returns a suitable preprocessor for the given env.

        This is a wrapper for get_preprocessor_for_space().
        """

        return ModelCatalog.get_preprocessor_for_space(
            env.observation_space, options, include_multi_binary
        )

    @staticmethod
    @DeveloperAPI
    def get_preprocessor_for_space(
        observation_space: gym.Space,
        options: dict = None,
        include_multi_binary: bool = False,
    ) -> Preprocessor:
        """Returns a suitable preprocessor for the given observation space.

        Args:
            observation_space: The input observation space.
            options: Options to pass to the preprocessor.
            include_multi_binary: Whether to include the MultiBinaryPreprocessor in
                the possible preprocessors returned by this method.

        Returns:
            preprocessor: Preprocessor for the observations.
        """

        options = options or MODEL_DEFAULTS
        for k in options.keys():
            if k not in MODEL_DEFAULTS:
                raise Exception(
                    "Unknown config key `{}`, all keys: {}".format(
                        k, list(MODEL_DEFAULTS)
                    )
                )

        cls = get_preprocessor(
            observation_space, include_multi_binary=include_multi_binary
        )
        prep = cls(observation_space, options)

        if prep is not None:
            logger.debug(
                "Created preprocessor {}: {} -> {}".format(
                    prep, observation_space, prep.shape
                )
            )
        return prep

    @staticmethod
    @PublicAPI
    def register_custom_model(model_name: str, model_class: type) -> None:
        """Register a custom model class by name.

        The model can be later used by specifying {"custom_model": model_name}
        in the model config.

        Args:
            model_name: Name to register the model under.
            model_class: Python class of the model.
        """
        if tf is not None:
            if issubclass(model_class, tf.keras.Model):
                deprecation_warning(old="register_custom_model", error=False)
        _global_registry.register(RLLIB_MODEL, model_name, model_class)

    @staticmethod
    @PublicAPI
    def register_custom_action_dist(
        action_dist_name: str, action_dist_class: type
    ) -> None:
        """Register a custom action distribution class by name.

        The model can be later used by specifying
        {"custom_action_dist": action_dist_name} in the model config.

        Args:
            model_name: Name to register the action distribution under.
            model_class: Python class of the action distribution.
        """
        _global_registry.register(
            RLLIB_ACTION_DIST, action_dist_name, action_dist_class
        )

    @staticmethod
    def _wrap_if_needed(model_cls: type, model_interface: type) -> type:
        if not model_interface or issubclass(model_cls, model_interface):
            return model_cls

        assert issubclass(model_cls, ModelV2), model_cls

        class wrapper(model_interface, model_cls):
            pass

        name = "{}_as_{}".format(model_cls.__name__, model_interface.__name__)
        wrapper.__name__ = name
        wrapper.__qualname__ = name

        return wrapper

    @staticmethod
    def _get_v2_model_class(
        input_space: gym.Space, model_config: ModelConfigDict, framework: str = "tf"
    ) -> Type[ModelV2]:
        VisionNet = None
        ComplexNet = None

        if framework in ["tf2", "tf"]:
            from ray.rllib.models.tf.fcnet import (
                FullyConnectedNetwork as FCNet,
            )
            from ray.rllib.models.tf.visionnet import (
                VisionNetwork as VisionNet,
            )
            from ray.rllib.models.tf.complex_input_net import (
                ComplexInputNetwork as ComplexNet,
            )
        elif framework == "torch":
            from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as FCNet
            from ray.rllib.models.torch.visionnet import VisionNetwork as VisionNet
            from ray.rllib.models.torch.complex_input_net import (
                ComplexInputNetwork as ComplexNet,
            )
        elif framework == "jax":
            from ray.rllib.models.jax.fcnet import FullyConnectedNetwork as FCNet
        else:
            raise ValueError(
                "framework={} not supported in `ModelCatalog._get_v2_model_"
                "class`!".format(framework)
            )

        orig_space = (
            input_space
            if not hasattr(input_space, "original_space")
            else input_space.original_space
        )

        # `input_space` is 3D Box -> VisionNet.
        if isinstance(input_space, Box) and len(input_space.shape) == 3:
            if framework == "jax":
                raise NotImplementedError("No non-FC default net for JAX yet!")
            return VisionNet
        # `input_space` is 1D Box -> FCNet.
        elif (
            isinstance(input_space, Box)
            and len(input_space.shape) == 1
            and (
                not isinstance(orig_space, (Dict, Tuple))
                or not any(
                    isinstance(s, Box) and len(s.shape) >= 2
                    for s in flatten_space(orig_space)
                )
            )
        ):
            return FCNet
        # Complex (Dict, Tuple, 2D Box (flatten), Discrete, MultiDiscrete).
        else:
            if framework == "jax":
                raise NotImplementedError("No non-FC default net for JAX yet!")
            return ComplexNet

    @staticmethod
    def _get_multi_action_distribution(dist_class, action_space, config, framework):
        # In case the custom distribution is a child of MultiActionDistr.
        # If users want to completely ignore the suggested child
        # distributions, they should simply do so in their custom class'
        # constructor.
        if issubclass(
            dist_class, (MultiActionDistribution, TorchMultiActionDistribution)
        ):
            flat_action_space = flatten_space(action_space)
            child_dists_and_in_lens = tree.map_structure(
                lambda s: ModelCatalog.get_action_dist(s, config, framework=framework),
                flat_action_space,
            )
            child_dists = [e[0] for e in child_dists_and_in_lens]
            input_lens = [int(e[1]) for e in child_dists_and_in_lens]
            return (
                partial(
                    dist_class,
                    action_space=action_space,
                    child_distributions=child_dists,
                    input_lens=input_lens,
                ),
                int(sum(input_lens)),
            )
        return dist_class, dist_class.required_model_output_shape(action_space, config)

    @staticmethod
    def _validate_config(
        config: ModelConfigDict, action_space: gym.spaces.Space, framework: str
    ) -> None:
        """Validates a given model config dict.

        Args:
            config: The "model" sub-config dict
                within the Algorithm's config dict.
            action_space: The action space of the model, whose config are
                    validated.
            framework: One of "jax", "tf2", "tf", or "torch".

        Raises:
            ValueError: If something is wrong with the given config.
        """
        # Soft-deprecate custom preprocessors.
        if config.get("custom_preprocessor") is not None:
            deprecation_warning(
                old="model.custom_preprocessor",
                new="gym.ObservationWrapper around your env or handle complex "
                "inputs inside your Model",
                error=True,
            )

        if config.get("use_attention") and config.get("use_lstm"):
            raise ValueError(
                "Only one of `use_lstm` or `use_attention` may be set to True!"
            )

        # For complex action spaces, only allow prev action inputs to
        # LSTMs and attention nets iff `_disable_action_flattening=True`.
        # TODO: `_disable_action_flattening=True` will be the default in
        #  the future.
        if (
            (
                config.get("lstm_use_prev_action")
                or config.get("attention_use_n_prev_actions", 0) > 0
            )
            and not config.get("_disable_action_flattening")
            and isinstance(action_space, (Tuple, Dict))
        ):
            raise ValueError(
                "For your complex action space (Tuple|Dict) and your model's "
                "`prev-actions` setup of your model, you must set "
                "`_disable_action_flattening=True` in your main config dict!"
            )

        if framework == "jax":
            if config.get("use_attention"):
                raise ValueError(
                    "`use_attention` not available for framework=jax so far!"
                )
            elif config.get("use_lstm"):
                raise ValueError("`use_lstm` not available for framework=jax so far!")
