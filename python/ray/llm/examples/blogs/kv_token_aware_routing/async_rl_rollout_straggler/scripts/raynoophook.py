"""Remove an inherited Ray working directory from benchmark runtime environments."""

from __future__ import annotations

from typing import Any


def hook(runtime_env: Any) -> Any:
    if isinstance(runtime_env, dict):
        return {
            key: value for key, value in runtime_env.items() if key != "working_dir"
        }
    return runtime_env
