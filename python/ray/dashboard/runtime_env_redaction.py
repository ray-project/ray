from typing import Any, Dict, List, Optional

import aiohttp.web

from ray._private.ray_constants import RAY_DASHBOARD_REDACT_RUNTIME_ENV
from ray._private.runtime_env.redaction import (  # noqa: F401
    REDACTED_PLACEHOLDER,
    redact_runtime_env,
    redact_serialized_runtime_env,
)
from ray.dashboard.optional_utils import is_browser_request

# Keys that carry a `runtime_env` in a response payload, and the shape they take.
# `worker_runtime_env` is the Train `RunConfig` field for per-worker runtime envs.
_RUNTIME_ENV_DICT_KEYS = ("runtime_env", "worker_runtime_env")
_SERIALIZED_RUNTIME_ENV_KEYS = ("serialized_runtime_env",)
_RUNTIME_ENV_INFO_KEYS = ("runtime_env_info",)


def should_redact_runtime_env(req: aiohttp.web.Request) -> bool:
    """Whether `req` must not be shown plaintext `runtime_env` secrets.

    Args:
        req: The incoming request.

    Returns:
        True if the request looks like it came from a browser and redaction is
        enabled.
    """
    return RAY_DASHBOARD_REDACT_RUNTIME_ENV and is_browser_request(req)


def redact_runtime_env_info(
    runtime_env_info: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Return a copy of a `RuntimeEnvInfo` dict with secret values masked."""
    if not isinstance(runtime_env_info, dict):
        return runtime_env_info

    redacted = dict(runtime_env_info)
    for key in _SERIALIZED_RUNTIME_ENV_KEYS:
        if key in redacted:
            redacted[key] = redact_serialized_runtime_env(redacted[key])
    return redacted


def redact_runtime_env_deep(payload: Any) -> Any:
    """Return a copy of `payload` with every nested `runtime_env` redacted.

    Unlike `redact_state_rows`, this walks arbitrarily nested structures. It is
    for small, deeply nested payloads such as the Serve config, where a
    `runtime_env` can appear at several levels (per application, per deployment
    under `ray_actor_options`, and on `controller_options`). Prefer the shallow
    helpers on hot list endpoints that can return thousands of rows.

    Args:
        payload: An arbitrary JSON-like structure.

    Returns:
        A copy with secret values masked. Non-container values are returned
        as-is.
    """
    if isinstance(payload, dict):
        redacted = {}
        for key, value in payload.items():
            if key in _RUNTIME_ENV_DICT_KEYS:
                redacted[key] = redact_runtime_env(value)
            elif key in _SERIALIZED_RUNTIME_ENV_KEYS:
                redacted[key] = redact_serialized_runtime_env(value)
            elif key in _RUNTIME_ENV_INFO_KEYS:
                redacted[key] = redact_runtime_env_info(value)
            else:
                redacted[key] = redact_runtime_env_deep(value)
        return redacted
    if isinstance(payload, list):
        return [redact_runtime_env_deep(item) for item in payload]
    return payload


def redact_state_rows(
    rows: Optional[List[Dict[str, Any]]]
) -> Optional[List[Dict[str, Any]]]:
    """Return state API rows with every `runtime_env`-bearing field redacted.

    Handles the three shapes a `runtime_env` takes across the state schemas:
    a plain dict (`RuntimeEnvState.runtime_env`), a serialized JSON string
    (`ActorState.serialized_runtime_env`), and a `RuntimeEnvInfo` dict holding a
    serialized string (`TaskState.runtime_env_info`).
    """
    if not rows:
        return rows

    redacted_rows = []
    for row in rows:
        if not isinstance(row, dict):
            redacted_rows.append(row)
            continue

        redacted_row = None

        def get_or_create_copy():
            nonlocal redacted_row
            if redacted_row is None:
                redacted_row = dict(row)
            return redacted_row

        for key in _RUNTIME_ENV_DICT_KEYS:
            if key in row:
                get_or_create_copy()[key] = redact_runtime_env(row[key])
        for key in _SERIALIZED_RUNTIME_ENV_KEYS:
            if key in row:
                get_or_create_copy()[key] = redact_serialized_runtime_env(row[key])
        for key in _RUNTIME_ENV_INFO_KEYS:
            if key in row:
                get_or_create_copy()[key] = redact_runtime_env_info(row[key])

        redacted_rows.append(redacted_row if redacted_row is not None else row)
    return redacted_rows
