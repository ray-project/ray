"""vLLM engine capability mixins.

Provides vLLM-specific implementations of engine capabilities.
"""

from ray.llm._internal.serve.engines.vllm.mixins.sleepable import (
    VLLMSleepableEngineMixin,
    VLLMSleepConfig,
    VLLMWakeupConfig,
)

__all__ = [
    "VLLMSleepableEngineMixin",
    "VLLMSleepConfig",
    "VLLMWakeupConfig",
]
