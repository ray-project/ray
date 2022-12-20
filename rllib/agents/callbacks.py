from ray.rllib.algorithms.callbacks import (  # noqa
    DefaultCallbacks,
    MemoryTrackingCallbacks,
    MultiCallbacks,
    RE3UpdateCallbacks,
)
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.callbacks",
    new="ray.rllib.algorithms.callbacks",
    error=True,
)
