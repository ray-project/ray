"""Lazy environment variable accessors for the video processing example."""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Iterable


def _maybe_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _int_env_getter(name: str, default: int) -> Callable[[], int]:
    def _getter() -> int:
        return _maybe_int(os.getenv(name), default)

    return _getter


_ENVIRONMENT_VARIABLES: Dict[str, Callable[[], Any]] = {
    "RAY_VIDEO_EXAMPLE_MAX_TARGETS": _int_env_getter(
        "RAY_VIDEO_EXAMPLE_MAX_TARGETS", 10_000
    ),
    "RAY_VIDEO_EXAMPLE_MAX_DECODE_FRAMES": _int_env_getter(
        "RAY_VIDEO_EXAMPLE_MAX_DECODE_FRAMES", 100_000
    ),
}


def __getattr__(name: str) -> Any:
    getter = _ENVIRONMENT_VARIABLES.get(name)
    if getter is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getter()


def __dir__() -> Iterable[str]:
    return sorted(_ENVIRONMENT_VARIABLES.keys())


__all__ = list(__dir__())
