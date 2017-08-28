from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym

from ray.rllib.models.action_dist import (
    Categorical, Deterministic, DiagGaussian)
from ray.rllib.models.preprocessors import (
    NoPreprocessor, AtariRamPreprocessor, AtariPixelPreprocessor)
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.visionnet import VisionNetwork


class ModelCatalog(object):
    """Registry of default models and action distributions for envs.

    Example:
        dist_class, dist_dim = ModelCatalog.get_action_dist(env.action_space)
        model = ModelCatalog.get_model(inputs, dist_dim)
        dist = dist_class(model.outputs)
        action_op = dist.sample()
    """

    @staticmethod
    def get_action_dist(action_space, dist_type=None):
        """Returns action distribution class and size for the given action space.

        Args:
            action_space (Space): Action space of the target gym env.
            dist_type (Optional[str]): Identifier of the action distribution.

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
    def get_preprocessor(env_name, obs_shape):
        """Returns a suitable processor for the given environment.

        Args:
            env_name (str): The name of the environment.
            obs_shape (tuple): The shape of the env observation space.

        Returns:
            preprocessor (Preprocessor): Preprocessor for the env observations.
        """

        ATARI_OBS_SHAPE = (210, 160, 3)
        ATARI_RAM_OBS_SHAPE = (128,)

        if obs_shape == ATARI_OBS_SHAPE:
            print("Assuming Atari pixel env, using AtariPixelPreprocessor.")
            return AtariPixelPreprocessor()
        elif obs_shape == ATARI_RAM_OBS_SHAPE:
            print("Assuming Atari ram env, using AtariRamPreprocessor.")
            return AtariRamPreprocessor()

        print("Non-atari env, not using any observation preprocessor.")
        return NoPreprocessor()
