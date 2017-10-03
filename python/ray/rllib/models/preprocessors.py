from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import cv2
import numpy as np


class Preprocessor(object):
    """Defines an abstract observation preprocessor function."""

    def __init__(self, options):
        self.options = options
        self._init()

    def _init(self):
        pass

    def transform_shape(self, obs_shape):
        """Returns the preprocessed observation shape."""
        raise NotImplementedError

    def transform(self, observation):
        """Returns the preprocessed observation."""
        raise NotImplementedError


class AtariPixelPreprocessor(Preprocessor):
    def _init(self):
        self.grayscale = self.options.get("grayscale", False)
        self.zero_mean = self.options.get("zero_mean", True)
        self.dim = self.options.get("dim", 80)

    def transform_shape(self, obs_shape):
        if self.grayscale:
            return (self.dim, self.dim, 1)
        else:
            return (self.dim, self.dim, 3)

    # TODO(ekl) why does this need to return an extra size-1 dim (the [None])
    def transform(self, observation):
        """Downsamples images from (210, 160, 3) by the configured factor."""
        scaled = observation[25:-25, :, :]
        if self.dim < 80:
            scaled = cv2.resize(scaled, (80, 80))
        # OpenAI: Resize by half, then down to 42x42 (essentially mipmapping).
        # If we resize directly we lose pixels that, when mapped to 42x42,
        # aren't close enough to the pixel boundary.
        scaled = cv2.resize(scaled, (self.dim, self.dim))
        if self.grayscale:
            scaled = scaled.mean(2)
            scaled = scaled.astype(np.float32)
            # Rescale needed for maintaining 1 channel
            scaled = np.reshape(scaled, [self.dim, self.dim, 1])
        if self.zero_mean:
            scaled = (scaled - 128) / 128
        else:
            scaled *= 1.0 / 255.0
        return scaled


# TODO(rliaw): Also should include the deepmind preprocessor
class AtariRamPreprocessor(Preprocessor):
    def transform_shape(self, obs_shape):
        return (128,)

    def transform(self, observation):
        return (observation - 128) / 128


class NoPreprocessor(Preprocessor):
    def transform_shape(self, obs_shape):
        return obs_shape

    def transform(self, observation):
        return observation
