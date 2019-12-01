from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import cv2
import logging
import numpy as np
import gym

from ray.rllib.utils.annotations import override, PublicAPI

ATARI_OBS_SHAPE = (210, 160, 3)
ATARI_RAM_OBS_SHAPE = (128, )
VALIDATION_INTERVAL = 100

logger = logging.getLogger(__name__)


@PublicAPI
class Preprocessor(object):
    """Defines an abstract observation preprocessor function.

    Attributes:
        shape (obj): Shape of the preprocessed output.
    """

    @PublicAPI
    def __init__(self, obs_space, options=None):
        legacy_patch_shapes(obs_space)
        self._obs_space = obs_space
        if not options:
            from ray.rllib.models.catalog import MODEL_DEFAULTS
            self._options = MODEL_DEFAULTS.copy()
        else:
            self._options = options
        self.shape = self._init_shape(obs_space, self._options)
        self._size = int(np.product(self.shape))
        self._i = 0

    @PublicAPI
    def _init_shape(self, obs_space, options):
        """Returns the shape after preprocessing."""
        raise NotImplementedError

    @PublicAPI
    def transform(self, observation):
        """Returns the preprocessed observation."""
        raise NotImplementedError

    def write(self, observation, array, offset):
        """Alternative to transform for more efficient flattening."""
        array[offset:offset + self._size] = self.transform(observation)

    def check_shape(self, observation):
        """Checks the shape of the given observation."""
        if self._i % VALIDATION_INTERVAL == 0:
            if type(observation) is list and isinstance(
                    self._obs_space, gym.spaces.Box):
                observation = np.array(observation)
            try:
                if not self._obs_space.contains(observation):
                    raise ValueError(
                        "Observation outside expected value range",
                        self._obs_space, observation)
            except AttributeError:
                raise ValueError(
                    "Observation for a Box/MultiBinary/MultiDiscrete space "
                    "should be an np.array, not a Python list.", observation)
        self._i += 1

    @property
    @PublicAPI
    def size(self):
        return self._size

    @property
    @PublicAPI
    def observation_space(self):
        obs_space = gym.spaces.Box(-1., 1., self.shape, dtype=np.float32)
        # Stash the unwrapped space so that we can unwrap dict and tuple spaces
        # automatically in model.py
        if (isinstance(self, TupleFlatteningPreprocessor)
                or isinstance(self, DictFlatteningPreprocessor)):
            obs_space.original_space = self._obs_space
        return obs_space


class GenericPixelPreprocessor(Preprocessor):
    """Generic image preprocessor.

    Note: for Atari games, use config {"preprocessor_pref": "deepmind"}
    instead for deepmind-style Atari preprocessing.
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space, options):
        self._grayscale = options.get("grayscale")
        self._zero_mean = options.get("zero_mean")
        self._dim = options.get("dim")
        if self._grayscale:
            shape = (self._dim, self._dim, 1)
        else:
            shape = (self._dim, self._dim, 3)

        return shape

    @override(Preprocessor)
    def transform(self, observation):
        """Downsamples images from (210, 160, 3) by the configured factor."""
        self.check_shape(observation)
        scaled = observation[25:-25, :, :]
        if self._dim < 84:
            scaled = cv2.resize(scaled, (84, 84))
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
        return scaled


class AtariRamPreprocessor(Preprocessor):
    @override(Preprocessor)
    def _init_shape(self, obs_space, options):
        return (128, )

    @override(Preprocessor)
    def transform(self, observation):
        self.check_shape(observation)
        return (observation - 128) / 128


class OneHotPreprocessor(Preprocessor):
    @override(Preprocessor)
    def _init_shape(self, obs_space, options):
        return (self._obs_space.n, )

    @override(Preprocessor)
    def transform(self, observation):
        self.check_shape(observation)
        arr = np.zeros(self._obs_space.n)
        arr[observation] = 1
        return arr

    @override(Preprocessor)
    def write(self, observation, array, offset):
        array[offset + observation] = 1


class NoPreprocessor(Preprocessor):
    @override(Preprocessor)
    def _init_shape(self, obs_space, options):
        return self._obs_space.shape

    @override(Preprocessor)
    def transform(self, observation):
        self.check_shape(observation)
        return observation

    @override(Preprocessor)
    def write(self, observation, array, offset):
        array[offset:offset + self._size] = np.array(
            observation, copy=False).ravel()

    @property
    @override(Preprocessor)
    def observation_space(self):
        return self._obs_space


class TupleFlatteningPreprocessor(Preprocessor):
    """Preprocesses each tuple element, then flattens it all into a vector.

    RLlib models will unpack the flattened output before _build_layers_v2().
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space, options):
        assert isinstance(self._obs_space, gym.spaces.Tuple)
        size = 0
        self.preprocessors = []
        for i in range(len(self._obs_space.spaces)):
            space = self._obs_space.spaces[i]
            logger.debug("Creating sub-preprocessor for {}".format(space))
            preprocessor = get_preprocessor(space)(space, self._options)
            self.preprocessors.append(preprocessor)
            size += preprocessor.size
        return (size, )

    @override(Preprocessor)
    def transform(self, observation):
        self.check_shape(observation)
        array = np.zeros(self.shape)
        self.write(observation, array, 0)
        return array

    @override(Preprocessor)
    def write(self, observation, array, offset):
        assert len(observation) == len(self.preprocessors), observation
        for o, p in zip(observation, self.preprocessors):
            p.write(o, array, offset)
            offset += p.size


class DictFlatteningPreprocessor(Preprocessor):
    """Preprocesses each dict value, then flattens it all into a vector.

    RLlib models will unpack the flattened output before _build_layers_v2().
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space, options):
        assert isinstance(self._obs_space, gym.spaces.Dict)
        size = 0
        self.preprocessors = []
        for space in self._obs_space.spaces.values():
            logger.debug("Creating sub-preprocessor for {}".format(space))
            preprocessor = get_preprocessor(space)(space, self._options)
            self.preprocessors.append(preprocessor)
            size += preprocessor.size
        return (size, )

    @override(Preprocessor)
    def transform(self, observation):
        self.check_shape(observation)
        array = np.zeros(self.shape)
        self.write(observation, array, 0)
        return array

    @override(Preprocessor)
    def write(self, observation, array, offset):
        if not isinstance(observation, OrderedDict):
            observation = OrderedDict(sorted(observation.items()))
        assert len(observation) == len(self.preprocessors), \
            (len(observation), len(self.preprocessors))
        for o, p in zip(observation.values(), self.preprocessors):
            p.write(o, array, offset)
            offset += p.size


@PublicAPI
def get_preprocessor(space):
    """Returns an appropriate preprocessor class for the given space."""

    legacy_patch_shapes(space)
    obs_shape = space.shape

    if isinstance(space, gym.spaces.Discrete):
        preprocessor = OneHotPreprocessor
    elif obs_shape == ATARI_OBS_SHAPE:
        preprocessor = GenericPixelPreprocessor
    elif obs_shape == ATARI_RAM_OBS_SHAPE:
        preprocessor = AtariRamPreprocessor
    elif isinstance(space, gym.spaces.Tuple):
        preprocessor = TupleFlatteningPreprocessor
    elif isinstance(space, gym.spaces.Dict):
        preprocessor = DictFlatteningPreprocessor
    else:
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
