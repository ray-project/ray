"""Helpers for masking secret `runtime_env` values before they leave the process.

These depend only on `ray._private.ray_constants`, so that Ray Core (log lines,
process command lines, error messages) and the dashboard (HTTP responses, events
sent to an external HTTP endpoint) can all use them.
"""
import json
import logging
from typing import Any, Dict, Optional

from ray._private import ray_constants

logger = logging.getLogger(__name__)

# Placeholder substituted for every redacted value. Keys are left intact so that
# operators can still see *which* variables are set.
REDACTED_PLACEHOLDER = "<redacted>"

# `runtime_env` fields whose values are treated as secrets.
SECRET_RUNTIME_ENV_FIELDS = ("env_vars",)

# Keys that carry a `runtime_env` in a payload, and the shape they take.
# `worker_runtime_env` is the Train `RunConfig` field for per-worker runtime envs.
RUNTIME_ENV_DICT_KEYS = ("runtime_env", "worker_runtime_env")
# `serializedRuntimeEnv` is how Ray event protos spell it once converted to JSON.
SERIALIZED_RUNTIME_ENV_KEYS = ("serialized_runtime_env", "serializedRuntimeEnv")
RUNTIME_ENV_INFO_KEYS = ("runtime_env_info",)


def is_runtime_env_redaction_enabled() -> bool:
    """Whether `RAY_DASHBOARD_REDACT_RUNTIME_ENV` is on (the default).

    Dashboard responses to browser requests and events sent to an external HTTP
    endpoint follow this setting. The runtime env agent logs, worker process
    command lines, and error messages are always redacted.

    Returns:
        True unless `RAY_DASHBOARD_REDACT_RUNTIME_ENV` was set to anything other
        than `1` or `true` (case-insensitive) when `ray._private.ray_constants`
        was imported.
    """
    return ray_constants.RAY_DASHBOARD_REDACT_RUNTIME_ENV


def redact_runtime_env(
    runtime_env: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Return a copy of `runtime_env` with secret values masked.

    Keys are preserved so the response still shows which variables are set.
    The input is never mutated: the same `RuntimeEnv` dicts are used to actually
    launch drivers and workers, so redaction must stay confined to the response.

    Args:
        runtime_env: A `runtime_env` dict or `RuntimeEnv`. Any other value,
            including None, is returned as-is.

    Returns:
        A shallow copy of `runtime_env` with every `env_vars` value replaced by
        `REDACTED_PLACEHOLDER`, or `runtime_env` itself if it isn't a dict.
    """
    if not isinstance(runtime_env, dict):
        return runtime_env

    redacted = dict(runtime_env)
    for field in SECRET_RUNTIME_ENV_FIELDS:
        value = redacted.get(field)
        if isinstance(value, dict):
            redacted[field] = {key: REDACTED_PLACEHOLDER for key in value}
    return redacted


def redact_serialized_runtime_env(
    serialized_runtime_env: Optional[str],
) -> Optional[str]:
    """Return `serialized_runtime_env` (a JSON string) with secret values masked.

    Also works for a serialized `RuntimeEnvContext`, which keeps `env_vars` at
    the top level too.

    Fails closed: anything we can't parse into a `runtime_env` dict is replaced
    wholesale, since we can't rule out that it holds secrets. Malformed JSON
    never raises.

    Args:
        serialized_runtime_env: A JSON-serialized runtime env or runtime env
            context. A non-string or empty value is returned as-is.

    Returns:
        The input unchanged if it's empty, not a string, or has no secret values
        to mask; `REDACTED_PLACEHOLDER` if it doesn't parse into a JSON object;
        and otherwise the re-serialized JSON, with sorted keys and every
        `env_vars` value masked.
    """
    if not isinstance(serialized_runtime_env, str) or not serialized_runtime_env:
        return serialized_runtime_env

    try:
        runtime_env = json.loads(serialized_runtime_env)
    except json.JSONDecodeError:
        logger.debug("Could not parse serialized_runtime_env; redacting it whole.")
        return REDACTED_PLACEHOLDER

    if not isinstance(runtime_env, dict):
        return REDACTED_PLACEHOLDER

    if not any(runtime_env.get(field) for field in SECRET_RUNTIME_ENV_FIELDS):
        # Nothing to mask, so keep the original string instead of re-serializing it.
        return serialized_runtime_env

    return json.dumps(redact_runtime_env(runtime_env), sort_keys=True)


def redact_runtime_env_info(
    runtime_env_info: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Return a copy of a `RuntimeEnvInfo` dict with secret values masked.

    Args:
        runtime_env_info: A `RuntimeEnvInfo` proto converted to a dict. Any other
            value, including None, is returned as-is.

    Returns:
        A shallow copy of `runtime_env_info` whose serialized runtime env is
        redacted with `redact_serialized_runtime_env`, or `runtime_env_info`
        itself if it isn't a dict.
    """
    if not isinstance(runtime_env_info, dict):
        return runtime_env_info

    redacted = dict(runtime_env_info)
    for key in SERIALIZED_RUNTIME_ENV_KEYS:
        if key in redacted:
            redacted[key] = redact_serialized_runtime_env(redacted[key])
    return redacted


def redact_runtime_env_deep(payload: Any) -> Any:
    """Return a copy of `payload` with every nested `runtime_env` redacted.

    This walks arbitrarily nested structures. It is for small, deeply nested
    payloads such as the Serve config, where a `runtime_env` can appear at
    several levels (per application, per deployment under `ray_actor_options`,
    and on `controller_options`). Prefer the shallow helpers on hot paths that
    can handle thousands of rows.

    Args:
        payload: An arbitrary JSON-like structure.

    Returns:
        A copy with secret values masked. Non-container values are returned
        as-is.

    Raises:
        RecursionError: If `payload` is nested deeper than Python's recursion
            limit.
    """
    if isinstance(payload, dict):
        redacted = {}
        for key, value in payload.items():
            if key in RUNTIME_ENV_DICT_KEYS:
                redacted[key] = redact_runtime_env(value)
            elif key in SERIALIZED_RUNTIME_ENV_KEYS:
                redacted[key] = redact_serialized_runtime_env(value)
            elif key in RUNTIME_ENV_INFO_KEYS:
                redacted[key] = redact_runtime_env_info(value)
            else:
                redacted[key] = redact_runtime_env_deep(value)
        return redacted
    if isinstance(payload, list):
        return [redact_runtime_env_deep(item) for item in payload]
    return payload
