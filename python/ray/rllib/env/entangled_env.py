from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class EntangledEnv(gym.Env):
    """Interface for one physical environment that hosts
    several logical environments."""

    def reset(self):
        raise Exception(
            'EntangledEnv: reset with provided env_i should be used')

    @PublicAPI
    def reset(self, env_i):
        """Resets the state of the environment and returns an initial observation.

        Returns: observation (object): the initial observation of the
            space.
        """

        raise NotImplementedError

    @PublicAPI
    def step(self, actions):
        """Run one timestep of the environment's dynamics. When end of
        episode is reached, you are responsible for calling `reset()`
        to reset this environment's state.

        Accepts an action and returns a dict
        env_i -> tuples (observation, reward, done, info).
        Args:
            actions (dict env_i -> object):
            list of actions provided by the environment
        Returns:
            dict env_i -> tuples (observation, reward, done, info).
            observation (object): agent's observation
                of the current environment
            reward (float) : amount of reward returned after previous action
            done (boolean): whether the episode has ended, in which case
                further step() calls will return undefined results
            info (dict): contains auxiliary diagnostic information
                (helpful for debugging, and sometimes learning)
        """

        raise NotImplementedError
