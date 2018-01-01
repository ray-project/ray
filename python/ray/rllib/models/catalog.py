from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym

from ray.tune.registry import RLLIB_MODEL, RLLIB_PREPROCESSOR, \
    _default_registry

from ray.rllib.models.action_dist import (
    Categorical, Deterministic, DiagGaussian)
from ray.rllib.models.preprocessors import (
    NoPreprocessor, AtariRamPreprocessor, AtariPixelPreprocessor,
    OneHotPreprocessor)
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.visionnet import VisionNetwork


MODEL_CONFIGS = [
    # === Built-in options ===
    "conv_filters",  # Number of filters
    "dim",  # Dimension for ATARI
    "grayscale",  # Converts ATARI frame to 1 Channel Grayscale image
    "zero_mean",  # Changes frame to range from [-1, 1] if true
    "extra_frameskip",  # (int) for number of frames to skip
    "fcnet_activation",  # Nonlinearity for fully connected net (tanh, relu)
    "fcnet_hiddens",  # Number of hidden layers for fully connected net
    "free_log_std",  # Documented in ray.rllib.models.Model
    "channel_major",  # Pytorch conv requires images to be channel-major

    # === Options for custom models ===
    "custom_preprocessor",  # Name of a custom preprocessor to use
    "custom_model",  # Name of a custom model to use
    "custom_options",  # Extra options to pass to the custom classes
]


class ModelCatalog(object):
    """Registry of models, preprocessors, and action distributions for envs.

    Examples:
        >>> prep = ModelCatalog.get_preprocessor(env)
        >>> observation = prep.transform(raw_observation)

        >>> dist_cls, dist_dim = ModelCatalog.get_action_dist(env.action_space)
        >>> model = ModelCatalog.get_model(registry, inputs, dist_dim)
        >>> dist = dist_cls(model.outputs)
        >>> action = dist.sample()
    """

    ATARI_OBS_SHAPE = (210, 160, 3)
    ATARI_RAM_OBS_SHAPE = (128,)

    @staticmethod
    def get_action_dist(action_space, dist_type=None):
        """Returns action distribution class and size for the given action space.

        Args:
            action_space (Space): Action space of the target gym env.
            dist_type (str): Optional identifier of the action distribution.

        Returns:
            dist_class (ActionDistribution): Python class of the distribution.
            dist_dim (int): The size of the input vector to the distribution.
        """

        if isinstance(action_space, gym.spaces.Box):
            if dist_type is None:
                return DiagGaussian, action_space.shape[0] * 2
            elif dist_type == 'deterministic':
                return Deterministic, action_space.shape[0]
        elif isinstance(action_space, gym.spaces.Discrete):
            return Categorical, action_space.n

        raise NotImplementedError(
            "Unsupported args: {} {}".format(action_space, dist_type))

    @staticmethod
    def get_model(registry, inputs, num_outputs, options=dict()):
        """Returns a suitable model conforming to given input and output specs.

        Args:
            registry (obj): Registry of named objects (ray.tune.registry).
            inputs (Tensor): The input tensor to the model.
            num_outputs (int): The size of the output vector of the model.
            options (dict): Optional args to pass to the model constructor.

        Returns:
            model (Model): Neural network model.
        """

        if "custom_model" in options:
            model = options["custom_model"]
            print("Using custom model {}".format(model))
            return registry.get(RLLIB_MODEL, model)(
                inputs, num_outputs, options)

        obs_rank = len(inputs.shape) - 1

        if obs_rank > 1:
            return VisionNetwork(inputs, num_outputs, options)

        return FullyConnectedNetwork(inputs, num_outputs, options)

    @staticmethod
    def get_torch_model(registry, input_shape, num_outputs, options=dict()):
        """Returns a PyTorch suitable model. This is currently only supported
        in A3C.

        Args:
            registry (obj): Registry of named objects (ray.tune.registry).
            input_shape (tuple): The input shape to the model.
            num_outputs (int): The size of the output vector of the model.
            options (dict): Optional args to pass to the model constructor.

        Returns:
            model (Model): Neural network model.
        """
        from ray.rllib.models.pytorch.fcnet import (
            FullyConnectedNetwork as PyTorchFCNet)
        from ray.rllib.models.pytorch.visionnet import (
            VisionNetwork as PyTorchVisionNet)

        if "custom_model" in options:
            model = options["custom_model"]
            print("Using custom torch model {}".format(model))
            return registry.get(RLLIB_MODEL, model)(
                input_shape, num_outputs, options)

        obs_rank = len(input_shape) - 1

        if obs_rank > 1:
            return PyTorchVisionNet(input_shape, num_outputs, options)

        return PyTorchFCNet(input_shape[0], num_outputs, options)

    @staticmethod
    def get_preprocessor(registry, env, options=dict()):
        """Returns a suitable processor for the given environment.

        Args:
            registry (obj): Registry of named objects (ray.tune.registry).
            env (gym.Env): The gym environment to preprocess.
            options (dict): Options to pass to the preprocessor.

        Returns:
            preprocessor (Preprocessor): Preprocessor for the env observations.
        """

        # For older gym versions that don't set shape for Discrete
        if not hasattr(env.observation_space, "shape") and \
                isinstance(env.observation_space, gym.spaces.Discrete):
            env.observation_space.shape = ()

        obs_shape = env.observation_space.shape

        for k in options.keys():
            if k not in MODEL_CONFIGS:
                raise Exception(
                    "Unknown config key `{}`, all keys: {}".format(
                        k, MODEL_CONFIGS))

        print("Observation shape is {}".format(obs_shape))

        if "custom_preprocessor" in options:
            preprocessor = options["custom_preprocessor"]
            print("Using custom preprocessor {}".format(preprocessor))
            return registry.get(RLLIB_PREPROCESSOR, preprocessor)(
                env.observation_space, options)

        if obs_shape == ():
            print("Using one-hot preprocessor for discrete envs.")
            preprocessor = OneHotPreprocessor
        elif obs_shape == ModelCatalog.ATARI_OBS_SHAPE:
            print("Assuming Atari pixel env, using AtariPixelPreprocessor.")
            preprocessor = AtariPixelPreprocessor
        elif obs_shape == ModelCatalog.ATARI_RAM_OBS_SHAPE:
            print("Assuming Atari ram env, using AtariRamPreprocessor.")
            preprocessor = AtariRamPreprocessor
        else:
            print("Not using any observation preprocessor.")
            preprocessor = NoPreprocessor

        return preprocessor(env.observation_space, options)

    @staticmethod
    def get_preprocessor_as_wrapper(registry, env, options=dict()):
        """Returns a preprocessor as a gym observation wrapper.

        Args:
            registry (obj): Registry of named objects (ray.tune.registry).
            env (gym.Env): The gym environment to wrap.
            options (dict): Options to pass to the preprocessor.

        Returns:
            wrapper (gym.ObservationWrapper): Preprocessor in wrapper form.
        """

        preprocessor = ModelCatalog.get_preprocessor(registry, env, options)
        return _RLlibPreprocessorWrapper(env, preprocessor)

    @staticmethod
    def register_custom_preprocessor(preprocessor_name, preprocessor_class):
        """Register a custom preprocessor class by name.

        The preprocessor can be later used by specifying
        {"custom_preprocessor": preprocesor_name} in the model config.

        Args:
            preprocessor_name (str): Name to register the preprocessor under.
            preprocessor_class (type): Python class of the preprocessor.
        """
        _default_registry.register(
            RLLIB_PREPROCESSOR, preprocessor_name, preprocessor_class)

    @staticmethod
    def register_custom_model(model_name, model_class):
        """Register a custom model class by name.

        The model can be later used by specifying {"custom_model": model_name}
        in the model config.

        Args:
            model_name (str): Name to register the model under.
            model_class (type): Python class of the model.
        """
        _default_registry.register(RLLIB_MODEL, model_name, model_class)


class _RLlibPreprocessorWrapper(gym.ObservationWrapper):
    """Adapts a RLlib preprocessor for use as an observation wrapper."""

    def __init__(self, env, preprocessor):
        super(_RLlibPreprocessorWrapper, self).__init__(env)
        self.preprocessor = preprocessor

        from gym.spaces.box import Box
        self.observation_space = Box(-1.0, 1.0, preprocessor.shape)

    def _observation(self, observation):
        return self.preprocessor.transform(observation)
