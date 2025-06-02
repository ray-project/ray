from collections import OrderedDict
import logging
import numpy as np
import gymnasium as gym
from typing import Any, List

from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.images import resize
from ray.rllib.utils.spaces.space_utils import convert_element_to_space_type

ATARI_OBS_SHAPE = (210, 160, 3)
ATARI_RAM_OBS_SHAPE = (128,)

# Only validate env observations vs the observation space every n times in a
# Preprocessor.
OBS_VALIDATION_INTERVAL = 100

logger = logging.getLogger(__name__)


@OldAPIStack
class Preprocessor:
    """Defines an abstract observation preprocessor function.

    Attributes:
        shape (List[int]): Shape of the preprocessed output.
    """

    def __init__(self, obs_space: gym.Space, options: dict = None):
        _legacy_patch_shapes(obs_space)
        self._obs_space = obs_space
        if not options:
            from ray.rllib.models.catalog import MODEL_DEFAULTS

            self._options = MODEL_DEFAULTS.copy()
        else:
            self._options = options
        self.shape = self._init_shape(obs_space, self._options)
        self._size = int(np.prod(self.shape))
        self._i = 0
        self._obs_for_type_matching = self._obs_space.sample()

    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        """Returns the shape after preprocessing."""
        raise NotImplementedError

    def transform(self, observation: TensorType) -> np.ndarray:
        """Returns the preprocessed observation."""
        raise NotImplementedError

    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        """Alternative to transform for more efficient flattening."""
        array[offset : offset + self._size] = self.transform(observation)

    def check_shape(self, observation: Any) -> None:
        """Checks the shape of the given observation."""
        if self._i % OBS_VALIDATION_INTERVAL == 0:
            # Convert lists to np.ndarrays.
            if type(observation) is list and isinstance(
                self._obs_space, gym.spaces.Box
            ):
                observation = np.array(observation).astype(np.float32)
            if not self._obs_space.contains(observation):
                observation = convert_element_to_space_type(
                    observation, self._obs_for_type_matching
                )
            try:
                if not self._obs_space.contains(observation):
                    raise ValueError(
                        "Observation ({} dtype={}) outside given space ({})!".format(
                            observation,
                            observation.dtype
                            if isinstance(self._obs_space, gym.spaces.Box)
                            else None,
                            self._obs_space,
                        )
                    )
            except AttributeError as e:
                raise ValueError(
                    "Observation for a Box/MultiBinary/MultiDiscrete space "
                    "should be an np.array, not a Python list.",
                    observation,
                ) from e
        self._i += 1

    @property
    def size(self) -> int:
        return self._size

    @property
    def observation_space(self) -> gym.Space:
        obs_space = gym.spaces.Box(-1.0, 1.0, self.shape, dtype=np.float32)
        # Stash the unwrapped space so that we can unwrap dict and tuple spaces
        # automatically in modelv2.py
        classes = (
            DictFlatteningPreprocessor,
            OneHotPreprocessor,
            RepeatedValuesPreprocessor,
            TupleFlatteningPreprocessor,
            AtariRamPreprocessor,
            GenericPixelPreprocessor,
        )
        if isinstance(self, classes):
            obs_space.original_space = self._obs_space
        return obs_space


@OldAPIStack
class GenericPixelPreprocessor(Preprocessor):
    """Generic image preprocessor.

    Note: for Atari games, use config {"preprocessor_pref": "deepmind"}
    instead for deepmind-style Atari preprocessing.
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        self._grayscale = options.get("grayscale")
        self._zero_mean = options.get("zero_mean")
        self._dim = options.get("dim")
        if self._grayscale:
            shape = (self._dim, self._dim, 1)
        else:
            shape = (self._dim, self._dim, 3)

        return shape

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        """Downsamples images from (210, 160, 3) by the configured factor."""
        self.check_shape(observation)
        scaled = observation[25:-25, :, :]
        if self._dim < 84:
            scaled = resize(scaled, height=84, width=84)
        # OpenAI: Resize by half, then down to 42x42 (essentially mipmapping).
        # If we resize directly we lose pixels that, when mapped to 42x42,
        # aren't close enough to the pixel boundary.
        scaled = resize(scaled, height=self._dim, width=self._dim)
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


@OldAPIStack
class AtariRamPreprocessor(Preprocessor):
    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        return (128,)

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        self.check_shape(observation)
        return (observation.astype("float32") - 128) / 128


@OldAPIStack
class OneHotPreprocessor(Preprocessor):
    """One-hot preprocessor for Discrete and MultiDiscrete spaces.

    .. testcode::
        :skipif: True

        self.transform(Discrete(3).sample())

    .. testoutput::

        np.array([0.0, 1.0, 0.0])

    .. testcode::
        :skipif: True

        self.transform(MultiDiscrete([2, 3]).sample())

    .. testoutput::

        np.array([0.0, 1.0, 0.0, 0.0, 1.0])
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        if isinstance(obs_space, gym.spaces.Discrete):
            return (self._obs_space.n,)
        else:
            return (np.sum(self._obs_space.nvec),)

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        self.check_shape(observation)
        return gym.spaces.utils.flatten(self._obs_space, observation).astype(np.float32)

    @override(Preprocessor)
    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        array[offset : offset + self.size] = self.transform(observation)


@OldAPIStack
class NoPreprocessor(Preprocessor):
    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        return self._obs_space.shape

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        self.check_shape(observation)
        return observation

    @override(Preprocessor)
    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        array[offset : offset + self._size] = np.array(observation, copy=False).ravel()

    @property
    @override(Preprocessor)
    def observation_space(self) -> gym.Space:
        return self._obs_space


@OldAPIStack
class MultiBinaryPreprocessor(Preprocessor):
    """Preprocessor that turns a MultiBinary space into a Box.

    Note: Before RLModules were introduced, RLlib's ModelCatalogV2 would produce
    ComplexInputNetworks that treat MultiBinary spaces as Boxes. This preprocessor is
    needed to get rid of the ComplexInputNetworks and use RLModules instead because
    RLModules lack the logic to handle MultiBinary or other non-Box spaces.
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        return self._obs_space.shape

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        # The shape stays the same, but the dtype changes.
        self.check_shape(observation)
        return observation.astype(np.float32)

    @override(Preprocessor)
    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        array[offset : offset + self._size] = np.array(observation, copy=False).ravel()

    @property
    @override(Preprocessor)
    def observation_space(self) -> gym.Space:
        obs_space = gym.spaces.Box(0.0, 1.0, self.shape, dtype=np.float32)
        obs_space.original_space = self._obs_space
        return obs_space


@OldAPIStack
class TupleFlatteningPreprocessor(Preprocessor):
    """Preprocesses each tuple element, then flattens it all into a vector.

    RLlib models will unpack the flattened output before _build_layers_v2().
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        assert isinstance(self._obs_space, gym.spaces.Tuple)
        size = 0
        self.preprocessors = []
        for i in range(len(self._obs_space.spaces)):
            space = self._obs_space.spaces[i]
            logger.debug("Creating sub-preprocessor for {}".format(space))
            preprocessor_class = get_preprocessor(space)
            if preprocessor_class is not None:
                preprocessor = preprocessor_class(space, self._options)
                size += preprocessor.size
            else:
                preprocessor = None
                size += int(np.prod(space.shape))
            self.preprocessors.append(preprocessor)
        return (size,)

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        self.check_shape(observation)
        array = np.zeros(self.shape, dtype=np.float32)
        self.write(observation, array, 0)
        return array

    @override(Preprocessor)
    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        assert len(observation) == len(self.preprocessors), observation
        for o, p in zip(observation, self.preprocessors):
            p.write(o, array, offset)
            offset += p.size


@OldAPIStack
class DictFlatteningPreprocessor(Preprocessor):
    """Preprocesses each dict value, then flattens it all into a vector.

    RLlib models will unpack the flattened output before _build_layers_v2().
    """

    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        assert isinstance(self._obs_space, gym.spaces.Dict)
        size = 0
        self.preprocessors = []
        for space in self._obs_space.spaces.values():
            logger.debug("Creating sub-preprocessor for {}".format(space))
            preprocessor_class = get_preprocessor(space)
            if preprocessor_class is not None:
                preprocessor = preprocessor_class(space, self._options)
                size += preprocessor.size
            else:
                preprocessor = None
                size += int(np.prod(space.shape))
            self.preprocessors.append(preprocessor)
        return (size,)

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        self.check_shape(observation)
        array = np.zeros(self.shape, dtype=np.float32)
        self.write(observation, array, 0)
        return array

    @override(Preprocessor)
    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        if not isinstance(observation, OrderedDict):
            observation = OrderedDict(sorted(observation.items()))
        assert len(observation) == len(self.preprocessors), (
            len(observation),
            len(self.preprocessors),
        )
        for o, p in zip(observation.values(), self.preprocessors):
            p.write(o, array, offset)
            offset += p.size


@OldAPIStack
class RepeatedValuesPreprocessor(Preprocessor):
    """Pads and batches the variable-length list value."""

    @override(Preprocessor)
    def _init_shape(self, obs_space: gym.Space, options: dict) -> List[int]:
        assert isinstance(self._obs_space, Repeated)
        child_space = obs_space.child_space
        self.child_preprocessor = get_preprocessor(child_space)(
            child_space, self._options
        )
        # The first slot encodes the list length.
        size = 1 + self.child_preprocessor.size * obs_space.max_len
        return (size,)

    @override(Preprocessor)
    def transform(self, observation: TensorType) -> np.ndarray:
        array = np.zeros(self.shape)
        if isinstance(observation, list):
            for elem in observation:
                self.child_preprocessor.check_shape(elem)
        else:
            pass  # ValueError will be raised in write() below.
        self.write(observation, array, 0)
        return array

    @override(Preprocessor)
    def write(self, observation: TensorType, array: np.ndarray, offset: int) -> None:
        if not isinstance(observation, (list, np.ndarray)):
            raise ValueError(
                "Input for {} must be list type, got {}".format(self, observation)
            )
        elif len(observation) > self._obs_space.max_len:
            raise ValueError(
                "Input {} exceeds max len of space {}".format(
                    observation, self._obs_space.max_len
                )
            )
        # The first slot encodes the list length.
        array[offset] = len(observation)
        for i, elem in enumerate(observation):
            offset_i = offset + 1 + i * self.child_preprocessor.size
            self.child_preprocessor.write(elem, array, offset_i)


@OldAPIStack
def get_preprocessor(space: gym.Space, include_multi_binary=False) -> type:
    """Returns an appropriate preprocessor class for the given space."""

    _legacy_patch_shapes(space)
    obs_shape = space.shape

    if isinstance(space, (gym.spaces.Discrete, gym.spaces.MultiDiscrete)):
        preprocessor = OneHotPreprocessor
    elif obs_shape == ATARI_OBS_SHAPE:
        logger.debug(
            "Defaulting to RLlib's GenericPixelPreprocessor because input "
            "space has the atari-typical shape {}. Turn this behaviour off by setting "
            "`preprocessor_pref=None` or "
            "`preprocessor_pref='deepmind'` or disabling the preprocessing API "
            "altogether with `_disable_preprocessor_api=True`.".format(ATARI_OBS_SHAPE)
        )
        preprocessor = GenericPixelPreprocessor
    elif obs_shape == ATARI_RAM_OBS_SHAPE:
        logger.debug(
            "Defaulting to RLlib's AtariRamPreprocessor because input "
            "space has the atari-typical shape {}. Turn this behaviour off by setting "
            "`preprocessor_pref=None` or "
            "`preprocessor_pref='deepmind' or disabling the preprocessing API "
            "altogether with `_disable_preprocessor_api=True`."
            "`.".format(ATARI_OBS_SHAPE)
        )
        preprocessor = AtariRamPreprocessor
    elif isinstance(space, gym.spaces.Tuple):
        preprocessor = TupleFlatteningPreprocessor
    elif isinstance(space, gym.spaces.Dict):
        preprocessor = DictFlatteningPreprocessor
    elif isinstance(space, Repeated):
        preprocessor = RepeatedValuesPreprocessor
    # We usually only want to include this when using RLModules
    elif isinstance(space, gym.spaces.MultiBinary) and include_multi_binary:
        preprocessor = MultiBinaryPreprocessor
    else:
        preprocessor = NoPreprocessor

    return preprocessor


def _legacy_patch_shapes(space: gym.Space) -> List[int]:
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
                shape = _legacy_patch_shapes(s)
                shapes.append(shape)
            space.shape = tuple(shapes)

    return space.shape
