from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import cv2
import numpy as np
import tensorflow as tf
import gym

class Preprocessor(object):
    """Defines an abstract observation preprocessor function.

    Attributes:
        shape (obj): Shape of the preprocessed output.
    """

    def __init__(self, obs_space, options):
        self._obs_space = obs_space
        self._options = options
        self._init()

    def _init(self):
        pass

    def transform(self, observation):
        """Returns the preprocessed observation."""
        raise NotImplementedError


class AtariPixelPreprocessor(Preprocessor):
    def _init(self):
        self._grayscale = self._options.get("grayscale", False)
        self._zero_mean = self._options.get("zero_mean", True)
        self._dim = self._options.get("dim", 80)
        self._channel_major = self._options.get("channel_major", False)
        if self._grayscale:
            self.shape = (self._dim, self._dim, 1)
        else:
            self.shape = (self._dim, self._dim, 3)

        # channel_major requires (# in-channels, row dim, col dim)
        if self._channel_major:
            self.shape = self.shape[-1:] + self.shape[:-1]

    # TODO(ekl) why does this need to return an extra size-1 dim (the [None])
    def transform(self, observation):
        """Downsamples images from (210, 160, 3) by the configured factor."""
        scaled = observation[25:-25, :, :]
        if self._dim < 80:
            scaled = cv2.resize(scaled, (80, 80))
        # OpenAI: Resize by half, then down to 42x42 (essentially mipmapping).
        # If we resize directly we lose pixels that, when mapped to 42x42,
        # aren't close enough to the pixel boundary.
        scaled = cv2.resize(scaled, (self._dim, self._dim))
        if self._grayscale:
            scaled = scaled.mean(2)
            scaled = scaled.astype(np.float32)
            # Rescale needed for maintaining 1 channel
            scaled = np.reshape(scaled, [self._dim, self._dim, 1])
        if self._zero_mean:
            scaled = (scaled - 128) / 128
        else:
            scaled *= 1.0 / 255.0
        if self._channel_major:
            scaled = np.reshape(scaled, self.shape)
        return scaled


# TODO(rliaw): Also should include the deepmind preprocessor
class AtariRamPreprocessor(Preprocessor):
    def _init(self):
        self.shape = (128,)

    def transform(self, observation):
        return (observation - 128) / 128


class OneHotPreprocessor(Preprocessor):
    def _init(self):
        assert self._obs_space.shape == ()
        self.shape = (self._obs_space.n,)

    def transform(self, observation):
        arr = np.zeros(self._obs_space.n)
        arr[observation] = 1
        return arr


class NoPreprocessor(Preprocessor):
    def _init(self):
        # FIXME (eugene) this is just to get things working
        # if isinstance(self._obs_space, list):
        #     self.shape = self._obs_space[0].shape
        # else:
        self.shape = self._obs_space.shape

    def transform(self, observation):
        return observation


class MultiAgentPreprocessor(Preprocessor, gym.Wrapper):
    """
    Wrapper that takes the observation spaces and flattens and then concatenates them (if there are multiple)
    This allows for easy passing around of multiagent spaces without requiring a list
    """
    def __init__(self, env, options):
        self.input_shaper = Reshaper(env.observation_space)
        self.output_shaper = Reshaper(env.action_space)
        super(MultiAgentPreprocessor, self).__init__(env, options)
        if isinstance(env.observation_space, list):
            self.n_agents = len(env.observation_space)
        else:
            self.n_agents = 1
        # temp
        self.observation_space = self.input_shaper.get_flat_box()
        self.action_space = self.output_shaper.get_flat_box()
        import ipdb; ipdb.set_trace()

    # @property
    # @overrides
    # def observation_space(self):
    #     return self.input_shaper.get_flat_box()
    #
    # @property
    # @overrides
    # def action_space(self):
    #     return self.output_shaper.get_flat_box()

    def split_input_tensor(self, tensor, axis=1):
        return self.input_shaper.split_tensor(tensor, axis)

    def split_output_tensor(self, tensor, axis=1):
        return self.output_shaper.split_tensor(tensor, axis)

    def split_output_number(self, number):
        return self.output_shaper.split_number(number)

    def split_along_agents(self, tensor, axis=-1):
        return tf.split(tensor, num_or_size_splits=self.n_agents, axis=axis)


    def get_action_dims(self):
        return self.output_shaper.get_slice_lengths()

    # need to overwrite step to flatten the observations
    def step(self, action):
        observation, reward, done, info = self.env.step(action)
        observation = np.asarray(observation).reshape(self.observation_space.shape[0])
        return observation, reward, done, info

    def reset(self):
        observation = np.asarray(self.env.reset())
        observation = observation.reshape(self.observation_space.shape[0])
        return observation


# FIXME (move this elsewhere in a bit)
class Reshaper(object):
    """
    This class keeps track of where in the flattened observation space we should be slicing and what the
    new shapes should be
    """
    # TODO(ev) support discrete action spaces
    def __init__(self, env_space):
        self.shapes = []
        self.slice_positions = []
        self.env_space = env_space
        if isinstance(env_space, list):
            for space in env_space:
                arr_shape = np.asarray(space.shape)
                self.shapes.append(arr_shape)
                if len(self.slice_positions) == 0:
                    self.slice_positions.append(np.product(arr_shape))
                else:
                    self.slice_positions.append(np.product(arr_shape) + self.slice_positions[-1])
        else:
            self.shapes.append(np.asarray(env_space.shape))
            self.slice_positions.append(np.product(env_space.shape))


    def get_flat_shape(self):
        import ipdb; ipdb.set_trace()
        return self.slice_positions[-1]


    def get_slice_lengths(self):
        diffed_list = np.diff(self.slice_positions).tolist()
        diffed_list.insert(0, self.slice_positions[0])
        return np.asarray(diffed_list)


    def get_flat_box(self):
        lows = []
        highs = []
        if isinstance(self.env_space, list):
            for i in range(len(self.env_space)):
                lows += self.env_space[i].low.tolist()
                highs += self.env_space[i].high.tolist()
            return gym.spaces.Box(np.asarray(lows), np.asarray(highs))
        else:
            return gym.spaces.Box(self.env_space.low, self.env_space.high)


    def split_tensor(self, tensor, axis=-1):
        # FIXME (ev) brittle
        # also, if its not a tes
        slice_rescale = int(tensor.shape.as_list()[axis] / int(np.sum(self.get_slice_lengths())))
        return tf.split(tensor, slice_rescale*self.get_slice_lengths(), axis=axis)


    def split_number(self, number):
        slice_rescale = int(number / int(np.sum(self.get_slice_lengths())))
        return slice_rescale*self.get_slice_lengths()


    def split_agents(self, tensor, axis=-1):
        return tf.split(tensor)
