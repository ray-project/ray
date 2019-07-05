from gym.spaces.space import Space
from gym.spaces.box import Box
from gym.spaces.discrete import Discrete
from gym.spaces.multi_discrete import MultiDiscrete
from gym.spaces.multi_binary import MultiBinary
from gym.spaces.tuple import Tuple
from gym.spaces.dict import Dict

from gym.spaces.utils import flatdim
from gym.spaces.utils import flatten
from gym.spaces.utils import unflatten

__all__ = ["Space", "Box", "Discrete", "MultiDiscrete", "MultiBinary", "Tuple", "Dict", "flatdim", "flatten", "unflatten"]
