"""vLLM sleepable engine mixin.

Implements sleep/wakeup functionality for vLLM engines.
Sleep mode offloads model weights to CPU and discards KV cache.
"""

from typing import Any, List, Optional

from pydantic import BaseModel, field_validator


class VLLMSleepConfig(BaseModel):
    """vLLM-specific configuration for sleep operation."""

    level: int = 1
    """Sleep level:
    - Level 1: Offload weights to CPU RAM, discard KV cache
    - Level 2: Discard both model weights and KV cache (deeper sleep)
    """

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: Any) -> int:
        if v not in (1, 2):
            raise ValueError("level must be 1 or 2")
        return v


class VLLMWakeupConfig(BaseModel):
    """vLLM-specific configuration for wakeup operation."""

    tags: Optional[List[str]] = None
    """Optional tags to selectively wake up components:
    - "weights": Restore model weights only
    - "kv_cache": Restore KV cache only
    - None: Restore everything
    """

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: Any) -> Optional[List[str]]:
        if v is not None:
            valid_tags = {"weights", "kv_cache"}
            for tag in v:
                if tag not in valid_tags:
                    raise ValueError(
                        f"Invalid tag '{tag}'. Must be one of: {valid_tags}"
                    )
        return v


class VLLMSleepableEngineMixin:
    """vLLM implementation of sleep/wakeup functionality.

    This mixin requires self._engine_client to be set to a vLLM engine client.
    """

    async def sleep(self, **kwargs: Any) -> None:
        """Put the vLLM engine to sleep.

        Args:
            **kwargs: Options parsed into VLLMSleepConfig.
                - level (int): Sleep level (1 or 2). Default 1.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        config = VLLMSleepConfig(**kwargs)
        await self._engine_client.sleep(level=config.level)

    async def wakeup(self, **kwargs: Any) -> None:
        """Wake up the vLLM engine from sleep mode.

        Args:
            **kwargs: Options parsed into VLLMWakeupConfig.
                - tags (List[str], optional): Components to wake up.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        config = VLLMWakeupConfig(**kwargs)
        await self._engine_client.wake_up(tags=config.tags)

    async def is_sleeping(self) -> bool:
        """Check whether the vLLM engine is currently sleeping.

        Returns:
            True if the engine is sleeping, False otherwise.
        """
        assert self._engine_client is not None, "engine_client is not initialized"
        return await self._engine_client.is_sleeping()
