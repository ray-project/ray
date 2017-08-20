from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Preprocessor(object):
    """Defines an abstract observation preprocessor function."""

    def transform_shape(self, obs_shape):
        """Returns the preprocessed observation shape."""
        raise NotImplementedError

    def transform(self, observation):
        """Returns the preprocessed observation."""
        raise NotImplementedError


class AtariPixelPreprocessor(Preprocessor):
    def transform_shape(self, obs_shape):
        return (80, 80, 3)

    # TODO(ekl) why does this need to return an extra size-1 dim (the [None])
    def transform(self, observation):
        """Downsamples images from (210, 160, 3) to (80, 80, 3)."""
        return (observation[25:-25:2, ::2, :][None] - 128) / 128


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
