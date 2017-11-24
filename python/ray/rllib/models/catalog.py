from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym

from ray.rllib.models.action_dist import (
    Categorical, Deterministic, DiagGaussian)
from ray.rllib.models.preprocessors import (
    NoPreprocessor, AtariRamPreprocessor, AtariPixelPreprocessor,
    OneHotPreprocessor)
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.visionnet import VisionNetwork


MODEL_CONFIGS = [
    "conv_filters",  # Number of filters
    "dim",  # Dimension for ATARI
    "grayscale",  # Converts ATARI frame to 1 Channel Grayscale image
    "zero_mean",  # Changes frame to range from [-1, 1] if true
    "extra_frameskip",  # (int) for number of frames to skip
    "fcnet_activation",  # Nonlinearity for fully connected net (tanh, relu)
    "fcnet_hiddens",  # Number of hidden layers for fully connected net
    "free_log_std",  # Documented in ray.rllib.models.Model
    "channel_major",  # Pytorch conv requires images to be channel-major
]


class ModelCatalog(object):
    """Registry of default models and action distributions for envs."""

    ATARI_OBS_SHAPE = (210, 160, 3)
    ATARI_RAM_OBS_SHAPE = (128,)

    _registered_preprocessor = dict()

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
    def get_model(inputs, num_outputs, options=dict()):
        """Returns a suitable model conforming to given input and output specs.

        Args:
            inputs (Tensor): The input tensor to the model.
            num_outputs (int): The size of the output vector of the model.
            options (dict): Optional args to pass to the model constructor.

        Returns:
            model (Model): Neural network model.
        """

        obs_rank = len(inputs.get_shape()) - 1

        if obs_rank > 1:
            return VisionNetwork(inputs, num_outputs, options)

        return FullyConnectedNetwork(inputs, num_outputs, options)

    @staticmethod
    def get_torch_model(input_shape, num_outputs, options=dict()):
        """Returns a PyTorch suitable model. This is currently only supported
        in A3C.

        Args:
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

        obs_rank = len(input_shape) - 1

        if obs_rank > 1:
            return PyTorchVisionNet(input_shape, num_outputs, options)

        return PyTorchFCNet(input_shape[0], num_outputs, options)

    @classmethod
    def get_preprocessor(cls, env, options=dict()):
        """Returns a suitable processor for the given environment.

        Args:
            env (gym.Env): The gym environment to preprocess.
            options (dict): Options to pass to the preprocessor.

        Returns:
            preprocessor (Preprocessor): Preprocessor for the env observations.
        """

        # For older gym versions that don't set shape for Discrete
        if not hasattr(env.observation_space, "shape") and \
                isinstance(env.observation_space, gym.spaces.Discrete):
            env.observation_space.shape = ()

        env_name = env.spec.id
        obs_shape = env.observation_space.shape

        for k in options.keys():
            if k not in MODEL_CONFIGS:
                raise Exception(
                    "Unknown config key `{}`, all keys: {}".format(
                        k, MODEL_CONFIGS))

        print("Observation shape is {}".format(obs_shape))

        if env_name in cls._registered_preprocessor:
            return cls._registered_preprocessor[env_name](options)

        if obs_shape == ():
            print("Using one-hot preprocessor for discrete envs.")
            preprocessor = OneHotPreprocessor
        elif obs_shape == cls.ATARI_OBS_SHAPE:
            print("Assuming Atari pixel env, using AtariPixelPreprocessor.")
            preprocessor = AtariPixelPreprocessor
        elif obs_shape == cls.ATARI_RAM_OBS_SHAPE:
            print("Assuming Atari ram env, using AtariRamPreprocessor.")
            preprocessor = AtariRamPreprocessor
        else:
            print("Non-atari env, not using any observation preprocessor.")
            preprocessor = NoPreprocessor

        return preprocessor(env.observation_space, options)

    @classmethod
    def get_preprocessor_as_wrapper(cls, env, options=dict()):
        """Returns a preprocessor as a gym observation wrapper.

        Args:
            env (gym.Env): The gym environment to wrap.
            options (dict): Options to pass to the preprocessor.

        Returns:
            wrapper (gym.ObservationWrapper): Preprocessor in wrapper form.
        """

        preprocessor = cls.get_preprocessor(env, options)
        return _RLlibPreprocessorWrapper(env, preprocessor)

    @classmethod
    def register_preprocessor(cls, env_name, preprocessor_class):
        """Register a preprocessor class for a specific environment.

        Args:
            env_name (str): Name of the gym env we register the
                preprocessor for.
            preprocessor_class (type):
                Python class of the distribution.
        """
        cls._registered_preprocessor[env_name] = preprocessor_class


class _RLlibPreprocessorWrapper(gym.ObservationWrapper):
    """Adapts a RLlib preprocessor for use as an observation wrapper."""

    def __init__(self, env, preprocessor):
        super(_RLlibPreprocessorWrapper, self).__init__(env)
        self.preprocessor = preprocessor

        from gym.spaces.box import Box
        self.observation_space = Box(-1.0, 1.0, preprocessor.shape)

    def _observation(self, observation):
        return self.preprocessor.transform(observation)
