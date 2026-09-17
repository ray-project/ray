"""Remove an inherited Ray runtime working directory."""

from __future__ import annotations

from typing import Any


def hook(runtime_env: Any) -> Any:
    """Return the runtime_env unchanged, minus any injected working_dir."""
    if isinstance(runtime_env, dict) and "working_dir" in runtime_env:
        runtime_env = {k: v for k, v in runtime_env.items() if k != "working_dir"}
    return runtime_env
