from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym

from ray.rllib.models.distributions import Categorical, DiagGaussian
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.models.visionnet import VisionNetwork


class ModelCatalog(object):
  """Registry of default models and output distributions for envs.

  Example:
    dist_class, dist_dim = ModelCatalog.get_output_dist(env.action_space)
    model = ModelCatalog.get_model(inputs, dist_dim)
    dist = dist_class(model.outputs)
    action_op = dist.sample()
  """

  @staticmethod
  def get_output_dist(action_space):
    if isinstance(action_space, gym.spaces.Box):
      return DiagGaussian, action_space.shape[0] * 2
    elif isinstance(action_space, gym.spaces.Discrete):
      return Categorical, action_space.n
    else:
      raise NotImplementedError(
          "Unsupported action space: " + str(action_space))

  @staticmethod
  def get_model(inputs, num_outputs):
    obs_rank = len(inputs.get_shape()) - 1
    if obs_rank == 1:
      return FullyConnectedNetwork(inputs, num_outputs)
    elif obs_rank == 2:
      return VisionNetwork(inputs, num_outputs)
    else:
      raise NotImplementedError(
          "Unsupported observation space: " + str(inputs.get_shape()))
