from typing import Any, Dict, List, Optional

import aiohttp.web

from ray._common.runtime_env_redaction import (  # noqa: F401
    REDACTED_PLACEHOLDER,
    RUNTIME_ENV_DICT_KEYS,
    RUNTIME_ENV_INFO_KEYS,
    SERIALIZED_RUNTIME_ENV_KEYS,
    is_runtime_env_redaction_enabled,
    redact_runtime_env,
    redact_runtime_env_deep,
    redact_runtime_env_info,
    redact_serialized_runtime_env,
)
from ray.dashboard.optional_utils import is_browser_request


def should_redact_runtime_env(req: aiohttp.web.Request) -> bool:
    """Whether `req` must not be shown plaintext `runtime_env` secrets.

    Args:
        req: The incoming request.

    Returns:
        True if the request looks like it came from a browser and redaction is
        enabled.
    """
    return is_runtime_env_redaction_enabled() and is_browser_request(req)


def redact_state_rows(
    rows: Optional[List[Dict[str, Any]]]
) -> Optional[List[Dict[str, Any]]]:
    """Return state API rows with every `runtime_env`-bearing field redacted.

    Handles the three shapes a `runtime_env` takes across the state schemas:
    a plain dict (`RuntimeEnvState.runtime_env`), a serialized JSON string
    (`ActorState.serialized_runtime_env`), and a `RuntimeEnvInfo` dict holding a
    serialized string (`TaskState.runtime_env_info`).

    Args:
        rows: State API rows as dicts. Rows that aren't dicts pass through
            unchanged, and an empty or None value is returned as-is.

    Returns:
        A new list in which each row holding a `runtime_env` field is replaced by
        a redacted shallow copy. Rows without one are the original objects.
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

        for key in RUNTIME_ENV_DICT_KEYS:
            if key in row:
                get_or_create_copy()[key] = redact_runtime_env(row[key])
        for key in SERIALIZED_RUNTIME_ENV_KEYS:
            if key in row:
                get_or_create_copy()[key] = redact_serialized_runtime_env(row[key])
        for key in RUNTIME_ENV_INFO_KEYS:
            if key in row:
                get_or_create_copy()[key] = redact_runtime_env_info(row[key])

        redacted_rows.append(redacted_row if redacted_row is not None else row)
    return redacted_rows
