"""This file contains temporary stubs for TensorDict, SpecDict, and ModelConfig.
This allows us to implement modules utilizing these APIs before the actual
changes land in master.
This file is to be removed once these modules are commited to master.
"""

from ray.rllib.utils.nested_dict import NestedDict


# TODO: Remove once TensorDict is in master
class TensorDict(NestedDict):
    pass


# TODO: Remove once ModelConfig is in master
class ModelConfig:
    name = "Bork"
