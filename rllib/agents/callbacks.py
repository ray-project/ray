from ray.rllib.algorithms.callbacks import (
    DefaultCallbacks,
    MemoryTrackingCallbacks,
    MultiCallbacks,
    RE3UpdateCallbacks,
)
from ray.rllib.utils.deprecation import deprecation_warning

DefaultCallbacks = DefaultCallbacks
MemoryTrackingCallbacks = MemoryTrackingCallbacks
MultiCallbacks = MultiCallbacks
RE3UpdateCallbacks = RE3UpdateCallbacks

deprecation_warning(
    old="ray.rllib.agents.callbacks",
    new="ray.rllib.algorithms.callbacks",
    error=False,
)
