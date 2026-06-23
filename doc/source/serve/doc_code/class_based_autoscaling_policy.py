# __begin_class_based_autoscaling_policy__
import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

from ray.serve.config import AutoscalingContext

logger = logging.getLogger("ray.serve")


class FileBasedAutoscalingPolicy:
    """Scale replicas based on a target written to a JSON file.

    A background asyncio task re-reads the file every ``poll_interval_s``
    seconds.  ``__call__`` returns the latest value on every autoscaling
    tick.  In production you could replace the file read with an HTTP
    call, a message-queue consumer, or any other async IO operation.
    """

    def __init__(self, file_path: str, poll_interval_s: float = 5.0):
        self._file_path = Path(file_path)
        self._poll_interval_s = poll_interval_s
        self._desired_replicas: int = 1
        self._task: asyncio.Task = None
        self._started: bool = False

    def _ensure_started(self) -> None:
        """Lazily start the background poll on the controller event loop."""
        if self._started:
            return
        self._started = True
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._poll_file())

    async def _poll_file(self) -> None:
        """Read the target replica count from the JSON file in a loop."""
        while True:
            try:
                text = self._file_path.read_text()
                data = json.loads(text)
                self._desired_replicas = int(data["replicas"])
            except Exception:
                pass  # Keep the last known value on failure.
            await asyncio.sleep(self._poll_interval_s)

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[int, Dict[str, Any]]:
        self._ensure_started()

        desired = self._desired_replicas
        return desired, {"last_polled_value": self._desired_replicas}


# __end_class_based_autoscaling_policy__
