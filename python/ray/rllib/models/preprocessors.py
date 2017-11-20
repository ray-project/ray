from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import cv2
import numpy as np


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
        self.shape = self._obs_space.shape

    def transform(self, observation):
        return observation
