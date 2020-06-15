from functools import partial
import gym
import logging
import numpy as np

from ray.tune.registry import RLLIB_MODEL, RLLIB_PREPROCESSOR, \
    RLLIB_ACTION_DIST, _global_registry
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.models.tf.fcnet_v1 import FullyConnectedNetwork
from ray.rllib.models.tf.lstm_v1 import LSTM
from ray.rllib.models.tf.modelv1_compat import make_v1_wrapper
from ray.rllib.models.tf.recurrent_net import LSTMWrapper
from ray.rllib.models.tf.tf_action_dist import Categorical, \
    Deterministic, DiagGaussian, Dirichlet, \
    MultiActionDistribution, MultiCategorical
from ray.rllib.models.tf.visionnet_v1 import VisionNetwork
from ray.rllib.models.torch.torch_action_dist import TorchCategorical, \
    TorchDeterministic, TorchDiagGaussian, \
    TorchMultiActionDistribution, TorchMultiCategorical
from ray.rllib.utils import try_import_tree
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.spaces.space_utils import flatten_space

tf = try_import_tf()
tree = try_import_tree()

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
MODEL_DEFAULTS = {
    # === Built-in options ===
    # Filter config. List of [out_channels, kernel, stride] for each filter
    "conv_filters": None,
    # Nonlinearity for built-in convnet
    "conv_activation": "relu",
    # Nonlinearity for fully connected net (tanh, relu)
    "fcnet_activation": "tanh",
    # Number of hidden layers for fully connected net
    "fcnet_hiddens": [256, 256],
    # For DiagGaussian action distributions, make the second half of the model
    # outputs floating bias variables instead of state-dependent. This only
    # has an effect is using the default fully connected net.
    "free_log_std": False,
    # Whether to skip the final linear layer used to resize the hidden layer
    # outputs to size `num_outputs`. If True, then the last hidden layer
    # should already match num_outputs.
    "no_final_linear": False,
    # Whether layers should be shared for the value function.
    "vf_share_layers": True,

    # == LSTM ==
    # Whether to wrap the model with an LSTM.
    "use_lstm": False,
    # Max seq len for training the LSTM, defaults to 20.
    "max_seq_len": 20,
    # Size of the LSTM cell.
    "lstm_cell_size": 256,
    # Whether to feed a_{t-1}, r_{t-1} to LSTM.
    "lstm_use_prev_action_reward": False,
    # When using modelv1 models with a modelv2 algorithm, you may have to
    # define the state shape here (e.g., [256, 256]).
    "state_shape": None,

    # == Atari ==
    # Whether to enable framestack for Atari envs
    "framestack": True,
    # Final resized frame dimension
    "dim": 84,
    # (deprecated) Converts ATARI frame to 1 Channel Grayscale image
    "grayscale": False,
    # (deprecated) Changes frame to range from [-1, 1] if true
    "zero_mean": True,

    # === Options for custom models ===
    # Name of a custom model to use
    "custom_model": None,
    # Extra options to pass to the custom classes.
    # These will be available in the Model's
    "custom_model_config": {},
    # Name of a custom action distribution to use.
    "custom_action_dist": None,
    # Custom preprocessors are deprecated. Please use a wrapper class around
    # your environment instead to preprocess observations.
    "custom_preprocessor": None,

    # Deprecated config keys.
    "custom_options": DEPRECATED_VALUE,
}
# __sphinx_doc_end__
# yapf: enable


@PublicAPI
class ModelCatalog:
    """Registry of models, preprocessors, and action distributions for envs.

    Examples:
        >>> prep = ModelCatalog.get_preprocessor(env)
        >>> observation = prep.transform(raw_observation)

        >>> dist_class, dist_dim = ModelCatalog.get_action_dist(
        ...     env.action_space, {})
        >>> model = ModelCatalog.get_model_v2(
        ...     obs_space, action_space, num_outputs, options)
        >>> dist = dist_class(model.outputs, model)
        >>> action = dist.sample()
    """

    @staticmethod
    @DeveloperAPI
    def get_action_dist(action_space,
                        config,
                        dist_type=None,
                        framework="tf",
                        **kwargs):
        """Returns a distribution class and size for the given action space.

        Args:
            action_space (Space): Action space of the target gym env.
            config (Optional[dict]): Optional model config.
            dist_type (Optional[str]): Identifier of the action distribution.
            framework (str): One of "tf", "tfe", or "torch".
            kwargs (dict): Optional kwargs to pass on to the Distribution's
                constructor.

        Returns:
            dist_class (ActionDistribution): Python class of the distribution.
            dist_dim (int): The size of the input vector to the distribution.
        """

        dist = None
        config = config or MODEL_DEFAULTS
        # Custom distribution given.
        if config.get("custom_action_dist"):
            action_dist_name = config["custom_action_dist"]
            logger.debug(
                "Using custom action distribution {}".format(action_dist_name))
            dist = _global_registry.get(RLLIB_ACTION_DIST, action_dist_name)
        # Dist_type is given directly as a class.
        elif type(dist_type) is type and \
                issubclass(dist_type, ActionDistribution) and \
                dist_type not in (
                MultiActionDistribution, TorchMultiActionDistribution):
            dist = dist_type
        # Box space -> DiagGaussian OR Deterministic.
        elif isinstance(action_space, gym.spaces.Box):
            if len(action_space.shape) > 1:
                raise UnsupportedSpaceException(
                    "Action space has multiple dimensions "
                    "{}. ".format(action_space.shape) +
                    "Consider reshaping this into a single dimension, "
                    "using a custom action distribution, "
                    "using a Tuple action space, or the multi-agent API.")
            # TODO(sven): Check for bounds and return SquashedNormal, etc..
            if dist_type is None:
                dist = TorchDiagGaussian if framework == "torch" \
                    else DiagGaussian
            elif dist_type == "deterministic":
                dist = TorchDeterministic if framework == "torch" \
                    else Deterministic
        # Discrete Space -> Categorical.
        elif isinstance(action_space, gym.spaces.Discrete):
            dist = TorchCategorical if framework == "torch" else Categorical
        # Tuple/Dict Spaces -> MultiAction.
        elif dist_type in (MultiActionDistribution,
                           TorchMultiActionDistribution) or \
                isinstance(action_space, (gym.spaces.Tuple, gym.spaces.Dict)):
            flat_action_space = flatten_space(action_space)
            child_dists_and_in_lens = tree.map_structure(
                lambda s: ModelCatalog.get_action_dist(
                    s, config, framework=framework), flat_action_space)
            child_dists = [e[0] for e in child_dists_and_in_lens]
            input_lens = [int(e[1]) for e in child_dists_and_in_lens]
            return partial(
                (TorchMultiActionDistribution
                 if framework == "torch" else MultiActionDistribution),
                action_space=action_space,
                child_distributions=child_dists,
                input_lens=input_lens), int(sum(input_lens))
        # Simplex -> Dirichlet.
        elif isinstance(action_space, Simplex):
            if framework == "torch":
                # TODO(sven): implement
                raise NotImplementedError(
                    "Simplex action spaces not supported for torch.")
            dist = Dirichlet
        # MultiDiscrete -> MultiCategorical.
        elif isinstance(action_space, gym.spaces.MultiDiscrete):
            dist = TorchMultiCategorical if framework == "torch" else \
                MultiCategorical
            return partial(dist, input_lens=action_space.nvec), \
                int(sum(action_space.nvec))
        # Unknown type -> Error.
        else:
            raise NotImplementedError("Unsupported args: {} {}".format(
                action_space, dist_type))

        return dist, dist.required_model_output_shape(action_space, config)

    @staticmethod
    @DeveloperAPI
    def get_action_shape(action_space):
        """Returns action tensor dtype and shape for the action space.

        Args:
            action_space (Space): Action space of the target gym env.
        Returns:
            (dtype, shape): Dtype and shape of the actions tensor.
        """

        if isinstance(action_space, gym.spaces.Discrete):
            return (tf.int64, (None, ))
        elif isinstance(action_space, (gym.spaces.Box, Simplex)):
            return (tf.float32, (None, ) + action_space.shape)
        elif isinstance(action_space, gym.spaces.MultiDiscrete):
            return (tf.as_dtype(action_space.dtype),
                    (None, ) + action_space.shape)
        elif isinstance(action_space, (gym.spaces.Tuple, gym.spaces.Dict)):
            flat_action_space = flatten_space(action_space)
            size = 0
            all_discrete = True
            for i in range(len(flat_action_space)):
                if isinstance(flat_action_space[i], gym.spaces.Discrete):
                    size += 1
                else:
                    all_discrete = False
                    size += np.product(flat_action_space[i].shape)
            size = int(size)
            return (tf.int64 if all_discrete else tf.float32, (None, size))
        else:
            raise NotImplementedError(
                "Action space {} not supported".format(action_space))

    @staticmethod
    @DeveloperAPI
    def get_action_placeholder(action_space, name="action"):
        """Returns an action placeholder consistent with the action space

        Args:
            action_space (Space): Action space of the target gym env.
            name (str): An optional string to name the placeholder by.
                Default: "action".
        Returns:
            action_placeholder (Tensor): A placeholder for the actions
        """

        dtype, shape = ModelCatalog.get_action_shape(action_space)

        return tf.placeholder(dtype, shape=shape, name=name)

    @staticmethod
    @DeveloperAPI
    def get_model_v2(obs_space,
                     action_space,
                     num_outputs,
                     model_config,
                     framework="tf",
                     name="default_model",
                     model_interface=None,
                     default_model=None,
                     **model_kwargs):
        """Returns a suitable model compatible with given spaces and output.

        Args:
            obs_space (Space): Observation space of the target gym env. This
                may have an `original_space` attribute that specifies how to
                unflatten the tensor into a ragged tensor.
            action_space (Space): Action space of the target gym env.
            num_outputs (int): The size of the output vector of the model.
            framework (str): One of "tf", "tfe", or "torch".
            name (str): Name (scope) for the model.
            model_interface (cls): Interface required for the model
            default_model (cls): Override the default class for the model. This
                only has an effect when not using a custom model
            model_kwargs (dict): args to pass to the ModelV2 constructor

        Returns:
            model (ModelV2): Model to use for the policy.
        """

        if model_config.get("custom_model"):

            if "custom_options" in model_config and \
                    model_config["custom_options"] != DEPRECATED_VALUE:
                deprecation_warning(
                    "model.custom_options",
                    "model.custom_model_config",
                    error=False)
                model_config["custom_model_config"] = \
                    model_config.pop("custom_options")

            if isinstance(model_config["custom_model"], type):
                model_cls = model_config["custom_model"]
            else:
                model_cls = _global_registry.get(RLLIB_MODEL,
                                                 model_config["custom_model"])

            # TODO(sven): Hard-deprecate Model(V1).
            if issubclass(model_cls, ModelV2):
                logger.info("Wrapping {} as {}".format(model_cls,
                                                       model_interface))
                model_cls = ModelCatalog._wrap_if_needed(
                    model_cls, model_interface)

                if framework in ["tf", "tfe"]:
                    # Track and warn if vars were created but not registered.
                    created = set()

                    def track_var_creation(next_creator, **kw):
                        v = next_creator(**kw)
                        created.add(v)
                        return v

                    with tf.variable_creator_scope(track_var_creation):
                        # Try calling with kwargs first (custom ModelV2 should
                        # accept these as kwargs, not get them from
                        # config["custom_model_config"] anymore).
                        try:
                            instance = model_cls(obs_space, action_space,
                                                 num_outputs, model_config,
                                                 name, **model_kwargs)
                        except TypeError as e:
                            # Keyword error: Try old way w/o kwargs.
                            if "__init__() got an unexpected " in e.args[0]:
                                logger.warning(
                                    "Custom ModelV2 should accept all custom "
                                    "options as **kwargs, instead of expecting"
                                    " them in config['custom_model_config']!")
                                instance = model_cls(obs_space, action_space,
                                                     num_outputs, model_config,
                                                     name)
                            # Other error -> re-raise.
                            else:
                                raise e
                    registered = set(instance.variables())
                    not_registered = set()
                    for var in created:
                        if var not in registered:
                            not_registered.add(var)
                    if not_registered:
                        raise ValueError(
                            "It looks like variables {} were created as part "
                            "of {} but does not appear in model.variables() "
                            "({}). Did you forget to call "
                            "model.register_variables() on the variables in "
                            "question?".format(not_registered, instance,
                                               registered))
                else:
                    # PyTorch automatically tracks nn.Modules inside the parent
                    # nn.Module's constructor.
                    # TODO(sven): Do this for TF as well.
                    instance = model_cls(obs_space, action_space, num_outputs,
                                         model_config, name, **model_kwargs)
                return instance
            # TODO(sven): Hard-deprecate Model(V1). This check will be
            #   superflous then.
            elif tf.executing_eagerly():
                raise ValueError(
                    "Eager execution requires a TFModelV2 model to be "
                    "used, however you specified a custom model {}".format(
                        model_cls))

        if framework in ["tf", "tfe"]:
            v2_class = None
            # Try to get a default v2 model.
            if not model_config.get("custom_model"):
                v2_class = default_model or ModelCatalog._get_v2_model_class(
                    obs_space, model_config, framework=framework)

            if model_config.get("use_lstm"):
                wrapped_cls = v2_class
                forward = wrapped_cls.forward
                v2_class = ModelCatalog._wrap_if_needed(
                    wrapped_cls, LSTMWrapper)
                v2_class._wrapped_forward = forward

            # fallback to a default v1 model
            if v2_class is None:
                if tf.executing_eagerly():
                    raise ValueError(
                        "Eager execution requires a TFModelV2 model to be "
                        "used, however there is no default V2 model for this "
                        "observation space: {}, use_lstm={}".format(
                            obs_space, model_config.get("use_lstm")))
                v2_class = make_v1_wrapper(ModelCatalog.get_model)
            # Wrap in the requested interface.
            wrapper = ModelCatalog._wrap_if_needed(v2_class, model_interface)
            return wrapper(obs_space, action_space, num_outputs, model_config,
                           name, **model_kwargs)
        elif framework == "torch":
            v2_class = \
                default_model or ModelCatalog._get_v2_model_class(
                    obs_space, model_config, framework=framework)
            if model_config.get("use_lstm"):
                from ray.rllib.models.torch.recurrent_net import LSTMWrapper \
                    as TorchLSTMWrapper
                wrapped_cls = v2_class
                forward = wrapped_cls.forward
                v2_class = ModelCatalog._wrap_if_needed(
                    wrapped_cls, TorchLSTMWrapper)
                v2_class._wrapped_forward = forward
            # Wrap in the requested interface.
            wrapper = ModelCatalog._wrap_if_needed(v2_class, model_interface)
            return wrapper(obs_space, action_space, num_outputs, model_config,
                           name, **model_kwargs)
        else:
            raise NotImplementedError(
                "Framework must be 'tf' or 'torch': {}".format(framework))

    @staticmethod
    @DeveloperAPI
    def get_preprocessor(env, options=None):
        """Returns a suitable preprocessor for the given env.

        This is a wrapper for get_preprocessor_for_space().
        """

        return ModelCatalog.get_preprocessor_for_space(env.observation_space,
                                                       options)

    @staticmethod
    @DeveloperAPI
    def get_preprocessor_for_space(observation_space, options=None):
        """Returns a suitable preprocessor for the given observation space.

        Args:
            observation_space (Space): The input observation space.
            options (dict): Options to pass to the preprocessor.

        Returns:
            preprocessor (Preprocessor): Preprocessor for the observations.
        """

        options = options or MODEL_DEFAULTS
        for k in options.keys():
            if k not in MODEL_DEFAULTS:
                raise Exception("Unknown config key `{}`, all keys: {}".format(
                    k, list(MODEL_DEFAULTS)))

        if options.get("custom_preprocessor"):
            preprocessor = options["custom_preprocessor"]
            logger.info("Using custom preprocessor {}".format(preprocessor))
            logger.warning(
                "DeprecationWarning: Custom preprocessors are deprecated, "
                "since they sometimes conflict with the built-in "
                "preprocessors for handling complex observation spaces. "
                "Please use wrapper classes around your environment "
                "instead of preprocessors.")
            prep = _global_registry.get(RLLIB_PREPROCESSOR, preprocessor)(
                observation_space, options)
        else:
            cls = get_preprocessor(observation_space)
            prep = cls(observation_space, options)

        logger.debug("Created preprocessor {}: {} -> {}".format(
            prep, observation_space, prep.shape))
        return prep

    @staticmethod
    @PublicAPI
    def register_custom_preprocessor(preprocessor_name, preprocessor_class):
        """Register a custom preprocessor class by name.

        The preprocessor can be later used by specifying
        {"custom_preprocessor": preprocesor_name} in the model config.

        Args:
            preprocessor_name (str): Name to register the preprocessor under.
            preprocessor_class (type): Python class of the preprocessor.
        """
        _global_registry.register(RLLIB_PREPROCESSOR, preprocessor_name,
                                  preprocessor_class)

    @staticmethod
    @PublicAPI
    def register_custom_model(model_name, model_class):
        """Register a custom model class by name.

        The model can be later used by specifying {"custom_model": model_name}
        in the model config.

        Args:
            model_name (str): Name to register the model under.
            model_class (type): Python class of the model.
        """
        _global_registry.register(RLLIB_MODEL, model_name, model_class)

    @staticmethod
    @PublicAPI
    def register_custom_action_dist(action_dist_name, action_dist_class):
        """Register a custom action distribution class by name.

        The model can be later used by specifying
        {"custom_action_dist": action_dist_name} in the model config.

        Args:
            model_name (str): Name to register the action distribution under.
            model_class (type): Python class of the action distribution.
        """
        _global_registry.register(RLLIB_ACTION_DIST, action_dist_name,
                                  action_dist_class)

    @staticmethod
    def _wrap_if_needed(model_cls, model_interface):
        assert issubclass(model_cls, ModelV2), model_cls

        if not model_interface or issubclass(model_cls, model_interface):
            return model_cls

        class wrapper(model_interface, model_cls):
            pass

        name = "{}_as_{}".format(model_cls.__name__, model_interface.__name__)
        wrapper.__name__ = name
        wrapper.__qualname__ = name

        return wrapper

    @staticmethod
    def get_model(input_dict,
                  obs_space,
                  action_space,
                  num_outputs,
                  options,
                  state_in=None,
                  seq_lens=None):
        """Deprecated: Use get_model_v2() instead."""

        deprecation_warning("get_model", "get_model_v2", error=False)
        assert isinstance(input_dict, dict)
        options = options or MODEL_DEFAULTS
        model = ModelCatalog._get_model(input_dict, obs_space, action_space,
                                        num_outputs, options, state_in,
                                        seq_lens)

        if options.get("use_lstm"):
            copy = dict(input_dict)
            copy["obs"] = model.last_layer
            feature_space = gym.spaces.Box(
                -1, 1, shape=(model.last_layer.shape[1], ))
            model = LSTM(copy, feature_space, action_space, num_outputs,
                         options, state_in, seq_lens)

        logger.debug(
            "Created model {}: ({} of {}, {}, {}, {}) -> {}, {}".format(
                model, input_dict, obs_space, action_space, state_in, seq_lens,
                model.outputs, model.state_out))

        model._validate_output_shape()
        return model

    @staticmethod
    def _get_model(input_dict, obs_space, action_space, num_outputs, options,
                   state_in, seq_lens):
        deprecation_warning("_get_model", "get_model_v2", error=False)
        if options.get("custom_model"):
            model = options["custom_model"]
            logger.debug("Using custom model {}".format(model))
            return _global_registry.get(RLLIB_MODEL, model)(
                input_dict,
                obs_space,
                action_space,
                num_outputs,
                options,
                state_in=state_in,
                seq_lens=seq_lens)

        obs_rank = len(input_dict["obs"].shape) - 1  # drops batch dim

        if obs_rank > 2:
            return VisionNetwork(input_dict, obs_space, action_space,
                                 num_outputs, options)

        return FullyConnectedNetwork(input_dict, obs_space, action_space,
                                     num_outputs, options)

    @staticmethod
    def _get_v2_model_class(obs_space, model_config, framework="tf"):
        if framework == "torch":
            from ray.rllib.models.torch.fcnet import (FullyConnectedNetwork as
                                                      FCNet)
            from ray.rllib.models.torch.visionnet import (VisionNetwork as
                                                          VisionNet)
        else:
            from ray.rllib.models.tf.fcnet import \
                FullyConnectedNetwork as FCNet
            from ray.rllib.models.tf.visionnet import \
                VisionNetwork as VisionNet

        # Discrete/1D obs-spaces.
        if isinstance(obs_space, gym.spaces.Discrete) or \
                len(obs_space.shape) <= 2:
            return FCNet
        # Default Conv2D net.
        else:
            return VisionNet

    @staticmethod
    def get_torch_model(obs_space,
                        num_outputs,
                        options=None,
                        default_model_cls=None):
        raise DeprecationWarning("Please use get_model_v2() instead.")
