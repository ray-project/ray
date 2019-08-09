from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np

from ray.rllib.utils.annotations import override, PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
class VectorEnv(object):
    """An environment that supports batch evaluation.

    Subclasses must define the following attributes:

    Attributes:
        action_space (gym.Space): Action space of individual envs.
        observation_space (gym.Space): Observation space of individual envs.
        num_envs (int): Number of envs in this vector env.
    """

    @staticmethod
    def wrap(make_env=None,
             existing_envs=None,
             num_envs=1,
             action_space=None,
             observation_space=None):
        return _VectorizedGymEnv(make_env, existing_envs or [], num_envs,
                                 action_space, observation_space)

    @PublicAPI
    def vector_reset(self):
        """Resets all environments.

        Returns:
            obs (list): Vector of observations from each environment.
        """
        raise NotImplementedError

    @PublicAPI
    def reset_at(self, index):
        """Resets a single environment.

        Returns:
            obs (obj): Observations from the resetted environment.
        """
        raise NotImplementedError

    @PublicAPI
    def vector_step(self, actions):
        """Vectorized step.

        Arguments:
            actions (list): Actions for each env.

        Returns:
            obs (list): New observations for each env.
            rewards (list): Reward values for each env.
            dones (list): Done values for each env.
            infos (list): Info values for each env.
        """
        raise NotImplementedError

    @PublicAPI
    def get_unwrapped(self):
        """Returns the underlying env instances."""
        raise NotImplementedError


class _VectorizedGymEnv(VectorEnv):
    """Internal wrapper for gym envs to implement VectorEnv.

    Arguments:
        make_env (func|None): Factory that produces a new gym env. Must be
            defined if the number of existing envs is less than num_envs.
        existing_envs (list): List of existing gym envs.
        num_envs (int): Desired num gym envs to keep total.
    """

    def __init__(self,
                 make_env,
                 existing_envs,
                 num_envs,
                 action_space=None,
                 observation_space=None):
        self.make_env = make_env
        self.envs = existing_envs
        self.num_envs = num_envs
        while len(self.envs) < self.num_envs:
            self.envs.append(self.make_env(len(self.envs)))
        self.action_space = action_space or self.envs[0].action_space
        self.observation_space = observation_space or \
            self.envs[0].observation_space

    @override(VectorEnv)
    def vector_reset(self):
        return [e.reset() for e in self.envs]

    @override(VectorEnv)
    def reset_at(self, index):
        return self.envs[index].reset()

    @override(VectorEnv)
    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(self.num_envs):
            obs, r, done, info = self.envs[i].step(actions[i])
            if not np.isscalar(r) or not np.isreal(r) or not np.isfinite(r):
                raise ValueError(
                    "Reward should be finite scalar, got {} ({})".format(
                        r, type(r)))
            if type(info) is not dict:
                raise ValueError("Info should be a dict, got {} ({})".format(
                    info, type(info)))
            obs_batch.append(obs)
            rew_batch.append(r)
            done_batch.append(done)
            info_batch.append(info)
        return obs_batch, rew_batch, done_batch, info_batch

    @override(VectorEnv)
    def get_unwrapped(self):
        return self.envs
