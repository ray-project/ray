from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import cv2
import numpy as np
import gym

ATARI_OBS_SHAPE = (210, 160, 3)
ATARI_RAM_OBS_SHAPE = (128,)


class Preprocessor(object):
    """Defines an abstract observation preprocessor function.

    Attributes:
        shape (obj): Shape of the preprocessed output.
    """

    def __init__(self, obs_space, options):
        legacy_patch_shapes(obs_space)
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


class AtariRamPreprocessor(Preprocessor):
    def _init(self):
        self.shape = (128,)

    def transform(self, observation):
        return (observation - 128) / 128


class OneHotPreprocessor(Preprocessor):
    def _init(self):
        self.shape = (self._obs_space.n,)

    def transform(self, observation):
        arr = np.zeros(self._obs_space.n)
        arr[observation] = 1
        return arr


class NoPreprocessor(Preprocessor):
    def _init(self):
        self.shape = self._obs_space.shape

    def transform(self, observation):
        return observation


class TupleFlatteningPreprocessor(Preprocessor):
    """Preprocesses each tuple element, then flattens it all into a vector.

    If desired, the vector output can be unpacked via tf.reshape() within a
    custom model to handle each component separately.
    """

    def _init(self):
        assert isinstance(self._obs_space, gym.spaces.Tuple)
        size = 0
        self.preprocessors = []
        for i in range(len(self._obs_space.spaces)):
            space = self._obs_space.spaces[i]
            print("Creating sub-preprocessor for", space)
            preprocessor = get_preprocessor(space)(space, self._options)
            self.preprocessors.append(preprocessor)
            size += np.product(preprocessor.shape)
        self.shape = (size,)

    def transform(self, observation):
        assert len(observation) == len(self.preprocessors), observation
        return np.concatenate([
            np.reshape(p.transform(o), [np.product(p.shape)])
            for (o, p) in zip(observation, self.preprocessors)])


def get_preprocessor(space):
    """Returns an appropriate preprocessor class for the given space."""

    legacy_patch_shapes(space)
    obs_shape = space.shape
    print("Observation shape is {}".format(obs_shape))

    if isinstance(space, gym.spaces.Discrete):
        print("Using one-hot preprocessor for discrete envs.")
        preprocessor = OneHotPreprocessor
    elif obs_shape == ATARI_OBS_SHAPE:
        print("Assuming Atari pixel env, using AtariPixelPreprocessor.")
        preprocessor = AtariPixelPreprocessor
    elif obs_shape == ATARI_RAM_OBS_SHAPE:
        print("Assuming Atari ram env, using AtariRamPreprocessor.")
        preprocessor = AtariRamPreprocessor
    elif isinstance(space, gym.spaces.Tuple):
        print("Using a TupleFlatteningPreprocessor")
        preprocessor = TupleFlatteningPreprocessor
    else:
        print("Not using any observation preprocessor.")
        preprocessor = NoPreprocessor

    return preprocessor


def legacy_patch_shapes(space):
    """Assigns shapes to spaces that don't have shapes.

    This is only needed for older gym versions that don't set shapes properly
    for Tuple and Discrete spaces.
    """

    if not hasattr(space, "shape"):
        if isinstance(space, gym.spaces.Discrete):
            space.shape = ()
        elif isinstance(space, gym.spaces.Tuple):
            shapes = []
            for s in space.spaces:
                shape = legacy_patch_shapes(s)
                shapes.append(shape)
            space.shape = tuple(shapes)

    return space.shape
