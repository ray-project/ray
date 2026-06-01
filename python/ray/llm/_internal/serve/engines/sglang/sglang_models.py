from typing import Any, List, Literal, Optional

from pydantic import BaseModel, field_validator


class SGLangSleepConfig(BaseModel):
    """SGLang-specific configuration for sleep operation."""

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


class SGLangWakeupConfig(BaseModel):
    """SGLang-specific configuration for wakeup operation."""

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


class SGLangPauseConfig(BaseModel):
    """SGLang-specific configuration for pause operation."""

    mode: Literal["abort", "wait", "keep"] = "abort"
    """Pause mode:
    - "abort" (default): Abort all in-flight requests immediately.
    - "wait": Wait for in-flight requests to complete before pausing.
    - "keep": Freeze requests in queue; they resume on continue_generation().
    """

    clear_cache: bool = True
    """Whether to clear KV and prefix caches after draining.
    Set to False to preserve cache for faster resume.
    """
