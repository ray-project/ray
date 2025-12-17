"""Ingress capability mixins.

Provides HTTP endpoint mixins for control plane operations.
"""

from ray.llm._internal.serve.core.ingress.mixins.cache_manager import (
    CacheManagerIngressMixin,
    ResetPrefixCacheRequest,
)
from ray.llm._internal.serve.core.ingress.mixins.sleepable import (
    IsSleepingResponse,
    SleepableIngressMixin,
    SleepRequest,
    WakeupRequest,
)

__all__ = [
    "CacheManagerIngressMixin",
    "ResetPrefixCacheRequest",
    "SleepableIngressMixin",
    "SleepRequest",
    "WakeupRequest",
    "IsSleepingResponse",
]
