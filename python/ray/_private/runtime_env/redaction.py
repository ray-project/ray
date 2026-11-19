"""Helpers for masking secret `runtime_env` values before they leave the process.

These are dependency-free so that both the dashboard (HTTP responses) and the
runtime env agent / reporter agent (log lines, process command lines) can use
them.
"""
import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger(__name__)

# Placeholder substituted for every redacted value. Keys are left intact so that
# operators can still see *which* variables are set.
REDACTED_PLACEHOLDER = "<redacted>"

# `runtime_env` fields whose values are treated as secrets.
SECRET_RUNTIME_ENV_FIELDS = ("env_vars",)

# `runtime_env` fields holding package URIs, which can embed credentials
# (`user:pass@host`) or presigned query strings.
URI_RUNTIME_ENV_FIELD = "working_dir"
URI_LIST_RUNTIME_ENV_FIELDS = ("py_modules", "java_jars")
# `runtime_env` fields whose strings can embed URLs, such as `--index-url` or
# `pkg @ git+https://token@host/repo.git` requirements.
PACKAGE_RUNTIME_ENV_FIELDS = ("pip", "uv", "conda")

_URL_IN_TEXT = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"]+")


def redact_uri(uri: Any) -> Any:
    """Return `uri` reduced to scheme, host, port and path.

    Drops userinfo, query and fragment, the parts that carry credentials and
    presigned-URL signatures. Values without a network location, such as local
    paths, are returned unchanged.
    """
    if not isinstance(uri, str):
        return uri
    try:
        parts = urlsplit(uri)
    except ValueError:
        # Malformed, e.g. an unclosed IPv6 bracket. Fail closed.
        return REDACTED_PLACEHOLDER
    if not parts.scheme or not parts.netloc:
        return uri
    host = parts.netloc.rpartition("@")[2]
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def redact_uris_in_text(text: str) -> str:
    """Return `text` with every URL in it reduced by `redact_uri`."""
    return _URL_IN_TEXT.sub(lambda match: redact_uri(match.group(0)), text)


def _redact_uris_deep(value: Any) -> Any:
    if isinstance(value, str):
        return redact_uris_in_text(value)
    if isinstance(value, list):
        return [_redact_uris_deep(item) for item in value]
    if isinstance(value, dict):
        return {key: _redact_uris_deep(item) for key, item in value.items()}
    return value


def redact_runtime_env(
    runtime_env: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Return a copy of `runtime_env` with secret values masked.

    `env_vars` keys are preserved so the response still shows which variables
    are set. Package URIs, and URLs inside `pip`, `uv` and `conda` specs, are
    reduced by `redact_uri`.
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
    if URI_RUNTIME_ENV_FIELD in redacted:
        redacted[URI_RUNTIME_ENV_FIELD] = redact_uri(redacted[URI_RUNTIME_ENV_FIELD])
    for field in URI_LIST_RUNTIME_ENV_FIELDS:
        value = redacted.get(field)
        if isinstance(value, list):
            redacted[field] = [redact_uri(uri) for uri in value]
    for field in PACKAGE_RUNTIME_ENV_FIELDS:
        if field in redacted:
            redacted[field] = _redact_uris_deep(redacted[field])
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
