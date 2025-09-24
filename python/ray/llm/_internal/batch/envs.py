"""Environment-driven tunables for Ray LLM batch stages.

Values are read lazily at runtime (per-process) so they can be controlled
via runtime_env or environment variables.
"""
from __future__ import annotations

import os
from typing import Callable


def _get_int_env(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


# Max number of sampling targets generated for fps mode.
RAY_LLM_BATCH_MAX_TARGETS: Callable[[], int] = (
    lambda: _get_int_env("RAY_LLM_BATCH_MAX_TARGETS", 10_000)
)

# Max decoded frames per source to avoid unbounded decoding.
RAY_LLM_BATCH_MAX_DECODE_FRAMES: Callable[[], int] = (
    lambda: _get_int_env("RAY_LLM_BATCH_MAX_DECODE_FRAMES", 100_000)
)
