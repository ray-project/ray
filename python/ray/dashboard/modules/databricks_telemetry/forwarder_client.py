"""HTTP client and local status mirror for the Databricks telemetry forwarder.

Modeled on ``ray._common.usage.usage_lib.UsageReportClient``. Intentionally
small: the head module owns retry/scheduling policy; this class only knows how
to POST a batch and atomically write the status mirror file.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from ray.dashboard.modules.databricks_telemetry.constants import STATUS_FILE

logger = logging.getLogger(__name__)


class ForwarderClient:
    """POST batches to the ingest endpoint and write the local status mirror.

    The class is stateless aside from a configurable request timeout so it can
    be reused across cycles without coordination.
    """

    def __init__(self, timeout_s: float = 30.0):
        self._timeout_s = timeout_s

    def post(
        self,
        url: str,
        batch: Dict[str, Any],
        token: Optional[str],
    ) -> None:
        """POST a single batch to the ingest endpoint.

        Raises ``requests.HTTPError`` (or another ``requests`` exception) on
        any non-2xx response or transport error. The caller is responsible
        for catching and accounting failures.
        """
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = requests.post(
            url,
            headers=headers,
            json=batch,
            timeout=self._timeout_s,
        )
        response.raise_for_status()

    def write_status_mirror(
        self,
        session_dir: str,
        status: Dict[str, Any],
    ) -> None:
        """Atomically write the per-cycle status file into ``session_dir``.

        Layout matches the ``usage_stats.json`` mirror pattern: write to a
        temp file in the same directory, then rename. Best-effort; failures
        are logged at debug level by callers (this method propagates).
        """
        dir_path = Path(session_dir)
        destination = dir_path / STATUS_FILE
        temp = dir_path / f"{STATUS_FILE}.tmp"
        with temp.open("w") as f:
            f.write(json.dumps(status))
        if sys.platform == "win32":
            # Windows doesn't support atomic rename onto an existing target.
            destination.unlink(missing_ok=True)
        temp.rename(destination)
