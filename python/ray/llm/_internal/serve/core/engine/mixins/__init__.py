"""Engine capability mixins.

Provides protocols for engine capabilities that can be composed.
"""

from ray.llm._internal.serve.core.engine.mixins.cache_manager import (
    CacheManagerEngineMixin,
)
from ray.llm._internal.serve.core.engine.mixins.sleepable import SleepableEngineMixin

__all__ = [
    "CacheManagerEngineMixin",
    "SleepableEngineMixin",
]
