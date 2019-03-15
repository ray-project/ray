from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import logging
import numpy as np
import tensorflow as tf
from functools import partial

from ray.tune.registry import RLLIB_MODEL, RLLIB_PREPROCESSOR, \
    _global_registry

from ray.rllib.models.extra_spaces import Simplex
from ray.rllib.models.action_dist import (Categorical, MultiCategorical,
                                          Deterministic, DiagGaussian,
                                          MultiActionDistribution, Dirichlet)
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.visionnet import VisionNetwork
from ray.rllib.models.lstm import LSTM
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI

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
    # For control envs, documented in ray.rllib.models.Model
    "free_log_std": False,
    # (deprecated) Whether to use sigmoid to squash actions to space range
    "squash_to_range": False,

    # == LSTM ==
    # Whether to wrap the model with a LSTM
    "use_lstm": False,
    # Max seq len for training the LSTM, defaults to 20
    "max_seq_len": 20,
    # Size of the LSTM cell
    "lstm_cell_size": 256,
    # Whether to feed a_{t-1}, r_{t-1} to LSTM
    "lstm_use_prev_action_reward": False,

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
    # Name of a custom preprocessor to use
    "custom_preprocessor": None,
    # Name of a custom model to use
    "custom_model": None,
    # Extra options to pass to the custom classes
    "custom_options": {},
}
# __sphinx_doc_end__
# yapf: enable


@PublicAPI
class ModelCatalog(object):
    """Registry of models, preprocessors, and action distributions for envs.

    Examples:
        >>> prep = ModelCatalog.get_preprocessor(env)
        >>> observation = prep.transform(raw_observation)

        >>> dist_cls, dist_dim = ModelCatalog.get_action_dist(
                env.action_space, {})
        >>> model = ModelCatalog.get_model(inputs, dist_dim, options)
        >>> dist = dist_cls(model.outputs)
        >>> action = dist.sample()
    """

    @staticmethod
    @DeveloperAPI
    def get_action_dist(action_space, config, dist_type=None):
        """Returns action distribution class and size for the given action space.

        Args:
            action_space (Space): Action space of the target gym env.
            config (dict): Optional model config.
            dist_type (str): Optional identifier of the action distribution.

        Returns:
            dist_class (ActionDistribution): Python class of the distribution.
            dist_dim (int): The size of the input vector to the distribution.
        """

        config = config or MODEL_DEFAULTS
        if isinstance(action_space, gym.spaces.Box):
            if len(action_space.shape) > 1:
                raise ValueError(
                    "Action space has multiple dimensions "
                    "{}. ".format(action_space.shape) +
                    "Consider reshaping this into a single dimension, "
                    "using a Tuple action space, or the multi-agent API.")
            if dist_type is None:
                dist = DiagGaussian
                if config.get("squash_to_range"):
                    raise ValueError(
                        "The squash_to_range option is deprecated. See the "
                        "clip_actions agent option instead.")
                return dist, action_space.shape[0] * 2
            elif dist_type == "deterministic":
                return Deterministic, action_space.shape[0]
        elif isinstance(action_space, gym.spaces.Discrete):
            return Categorical, action_space.n
        elif isinstance(action_space, gym.spaces.Tuple):
            child_dist = []
            input_lens = []
            for action in action_space.spaces:
                dist, action_size = ModelCatalog.get_action_dist(
                    action, config)
                child_dist.append(dist)
                input_lens.append(action_size)
            return partial(
                MultiActionDistribution,
                child_distributions=child_dist,
                action_space=action_space,
                input_lens=input_lens), sum(input_lens)
        elif isinstance(action_space, Simplex):
            return Dirichlet, action_space.shape[0]
        elif isinstance(action_space, gym.spaces.multi_discrete.MultiDiscrete):
            return MultiCategorical, sum(action_space.nvec)

        raise NotImplementedError("Unsupported args: {} {}".format(
            action_space, dist_type))

    @staticmethod
    @DeveloperAPI
    def get_action_placeholder(action_space):
        """Returns an action placeholder that is consistent with the action space

        Args:
            action_space (Space): Action space of the target gym env.
        Returns:
            action_placeholder (Tensor): A placeholder for the actions
        """

        if isinstance(action_space, gym.spaces.Box):
            return tf.placeholder(
                tf.float32, shape=(None, action_space.shape[0]), name="action")
        elif isinstance(action_space, gym.spaces.Discrete):
            return tf.placeholder(tf.int64, shape=(None, ), name="action")
        elif isinstance(action_space, gym.spaces.Tuple):
            size = 0
            all_discrete = True
            for i in range(len(action_space.spaces)):
                if isinstance(action_space.spaces[i], gym.spaces.Discrete):
                    size += 1
                else:
                    all_discrete = False
                    size += np.product(action_space.spaces[i].shape)
            return tf.placeholder(
                tf.int64 if all_discrete else tf.float32,
                shape=(None, size),
                name="action")
        elif isinstance(action_space, Simplex):
            return tf.placeholder(
                tf.float32, shape=(None, action_space.shape[0]), name="action")
        elif isinstance(action_space, gym.spaces.multi_discrete.MultiDiscrete):
            return tf.placeholder(
                tf.as_dtype(action_space.dtype),
                shape=(None, len(action_space.nvec)),
                name="action")
        else:
            raise NotImplementedError("action space {}"
                                      " not supported".format(action_space))

    @staticmethod
    @DeveloperAPI
    def get_model(input_dict,
                  obs_space,
                  action_space,
                  num_outputs,
                  options,
                  state_in=None,
                  seq_lens=None):
        """Returns a suitable model conforming to given input and output specs.

        Args:
            input_dict (dict): Dict of input tensors to the model, including
                the observation under the "obs" key.
            obs_space (Space): Observation space of the target gym env.
            action_space (Space): Action space of the target gym env.
            num_outputs (int): The size of the output vector of the model.
            options (dict): Optional args to pass to the model constructor.
            state_in (list): Optional RNN state in tensors.
            seq_lens (Tensor): Optional RNN sequence length tensor.

        Returns:
            model (models.Model): Neural network model.
        """

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

        obs_rank = len(input_dict["obs"].shape) - 1

        if obs_rank > 1:
            return VisionNetwork(input_dict, obs_space, action_space,
                                 num_outputs, options)

        return FullyConnectedNetwork(input_dict, obs_space, action_space,
                                     num_outputs, options)

    @staticmethod
    @DeveloperAPI
    def get_torch_model(obs_space,
                        num_outputs,
                        options=None,
                        default_model_cls=None):
        """Returns a custom model for PyTorch algorithms.

        Args:
            obs_space (Space): The input observation space.
            num_outputs (int): The size of the output vector of the model.
            options (dict): Optional args to pass to the model constructor.
            default_model_cls (cls): Optional class to use if no custom model.

        Returns:
            model (models.Model): Neural network model.
        """
        from ray.rllib.models.pytorch.fcnet import (FullyConnectedNetwork as
                                                    PyTorchFCNet)
        from ray.rllib.models.pytorch.visionnet import (VisionNetwork as
                                                        PyTorchVisionNet)

        options = options or MODEL_DEFAULTS

        if options.get("custom_model"):
            model = options["custom_model"]
            logger.debug("Using custom torch model {}".format(model))
            return _global_registry.get(RLLIB_MODEL,
                                        model)(obs_space, num_outputs, options)

        if options.get("use_lstm"):
            raise NotImplementedError(
                "LSTM auto-wrapping not implemented for torch")

        if default_model_cls:
            return default_model_cls(obs_space, num_outputs, options)

        if isinstance(obs_space, gym.spaces.Discrete):
            obs_rank = 1
        else:
            obs_rank = len(obs_space.shape)

        if obs_rank > 1:
            return PyTorchVisionNet(obs_space, num_outputs, options)

        return PyTorchFCNet(obs_space, num_outputs, options)

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
