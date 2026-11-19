"""Helpers for masking secret `runtime_env` values before they leave the process.

These are dependency-free so that both the dashboard (HTTP responses) and the
runtime env agent / reporter agent (log lines, process command lines) can use
them.
"""
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Placeholder substituted for every redacted value. Keys are left intact so that
# operators can still see *which* variables are set.
REDACTED_PLACEHOLDER = "<redacted>"

# `runtime_env` fields whose values are treated as secrets.
SECRET_RUNTIME_ENV_FIELDS = ("env_vars",)


def redact_runtime_env(
    runtime_env: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Return a copy of `runtime_env` with secret values masked.

    Keys are preserved so the response still shows which variables are set.
    The input is never mutated: the same `RuntimeEnv` dicts are used to actually
    launch drivers and workers, so redaction must stay confined to the response.
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

    if not any(
        isinstance(runtime_env.get(field), dict) and runtime_env[field]
        for field in SECRET_RUNTIME_ENV_FIELDS
    ):
        # Nothing to mask, so keep the original string instead of re-serializing it.
        return serialized_runtime_env

    # Match the compact, sorted, non-ASCII-preserving output of
    # `RedactSerializedRuntimeEnv` in src/ray/common/runtime_env_redaction.cc.
    return json.dumps(
        redact_runtime_env(runtime_env),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
