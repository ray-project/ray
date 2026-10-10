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
    wholesale, since we can't rule out that it holds secrets.
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

    return json.dumps(redact_runtime_env(runtime_env), sort_keys=True)
