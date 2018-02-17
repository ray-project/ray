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
        self._grayscale = self._options.get("grayscale", True)
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

    def transform(self, frame):
        """Downsamples images from (210, 160, 3) by the configured factor."""

        if frame.size == 210 * 160 * 3:
            img = np.reshape(frame, [210, 160, 3]).astype(np.float32)
        elif frame.size == 250 * 160 * 3:
            img = np.reshape(frame, [250, 160, 3]).astype(np.float32)
        else:
            assert False, "Unknown resolution."
        img = (img[:, :, 0] * 0.299 + img[:, :, 1] * 0.587 +
               img[:, :, 2] * 0.114)
        resized_screen = cv2.resize(
            img, (80, 110), interpolation=cv2.INTER_AREA)
        x_t = resized_screen[20:100, :]
        x_t = np.reshape(x_t, [80, 80, 1])
        return x_t.astype(np.uint8)


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
