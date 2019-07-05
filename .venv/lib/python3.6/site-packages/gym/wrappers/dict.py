import gym
import numpy as np


__all__ = ['FlattenDictWrapper']


class FlattenDictWrapper(gym.ObservationWrapper):
    """Flattens selected keys of a Dict observation space into
    an array.
    """
    def __init__(self, env, dict_keys):
        super(FlattenDictWrapper, self).__init__(env)
        self.dict_keys = dict_keys

        # Figure out observation_space dimension.
        size = 0
        for key in dict_keys:
            shape = self.env.observation_space.spaces[key].shape
            size += np.prod(shape)
        self.observation_space = gym.spaces.Box(-np.inf, np.inf, shape=(size,), dtype='float32')

    def observation(self, observation):
        assert isinstance(observation, dict)
        obs = []
        for key in self.dict_keys:
            obs.append(observation[key].ravel())
        return np.concatenate(obs)
