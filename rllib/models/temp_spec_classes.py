"""This file contains temporary stubs for TensorDict, SpecDict, and ModelConfig.
This allows us to implement modules utilizing these APIs before the actual
changes land in master.
This file is to be removed once these modules are commited to master.
"""

from dataclasses import dataclass
from ray.rllib.utils.nested_dict import NestedDict


# TODO: Remove once we have a proper encoder config within ModelConfig
@dataclass
class EncoderConfig:
    input_size: int
    hidden_size: int


# TODO: Remove once TensorDict is in master
class TensorDict(NestedDict):
    pass


# TODO: Remove once ModelConfig is in master
class ModelConfig:
    name = "Bork"
    encoder_config = EncoderConfig(input_size=32, hidden_size=64)
