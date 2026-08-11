import json
import logging
import logging.handlers
import os
import threading
import time
from typing import Any, Dict, Optional

from ray._common.utils import env_integer

logger = logging.getLogger(__name__)
_logger_lock = threading.Lock()

# Annotation files are size-bounded and rotated for long-running processes
RAY_ANNOTATION_MAX_FILE_SIZE_BYTES = env_integer(
    "RAY_ANNOTATION_MAX_FILE_SIZE_BYTES", 100 * 1000 * 1000
)
RAY_ANNOTATION_MAX_BACKUP_COUNT = env_integer("RAY_ANNOTATION_MAX_BACKUP_COUNT", 5)


class _AnnotationFileHandler(logging.Handler):
    """Write annotation records to a per-process file in the Ray session logs dir.

    The session logs directory is only known once Ray is initialized and
    rotated once it reaches ``RAY_ANNOTATION_MAX_FILE_SIZE_BYTES``,
    keeping ``RAY_ANNOTATION_MAX_BACKUP_COUNT`` previous files.
    """

    def __init__(self):
        super().__init__()
        self._handler: Optional[logging.handlers.RotatingFileHandler] = None
        self._logs_dir: Optional[str] = None

        self._setup_failure_reported: bool = False

    def emit(self, record: logging.LogRecord) -> None:
        from ray._private.worker import _global_node

        if _global_node is None:
            # Ray is not initialized yet; retry on the next emit.
            return

        try:
            logs_dir = _global_node.get_logs_dir_path()

            if self._handler is None or self._logs_dir != logs_dir:
                # The session logs dir changed (e.g. after a shutdown/restart).
                if self._handler is not None:
                    # Flush and close the previous handler
                    self._handler.close()
                    self._handler = None

                os.makedirs(logs_dir, exist_ok=True)
                filename = f"annotations_{os.getpid()}.log"
                self._handler = logging.handlers.RotatingFileHandler(
                    os.path.join(logs_dir, filename),
                    maxBytes=RAY_ANNOTATION_MAX_FILE_SIZE_BYTES,
                    backupCount=RAY_ANNOTATION_MAX_BACKUP_COUNT,
                    encoding="utf-8",
                )
                self._handler.setFormatter(self.formatter)
                self._logs_dir = logs_dir

            # Snapshot the handler so a concurrent `close()` (e.g. from
            # `logging.shutdown()` at interpreter exit) can't null it out
            # between here and the `emit` below.
            handler = self._handler
        except Exception:
            if not self._setup_failure_reported:
                self._setup_failure_reported = True
                logger.warning(
                    "Failed to open the annotation log file, so annotations will not be "
                    "written. Emitting will keep being retried, but further failures "
                    "will not be logged.",
                    exc_info=True,
                )
            return

        if handler is not None:
            handler.emit(record)

    def flush(self) -> None:
        if self._handler is not None:
            self._handler.flush()
        super().flush()

    def close(self) -> None:
        if self._handler is not None:
            self._handler.close()
            self._handler = None
            self._logs_dir = None
        super().close()


class Annotation:
    """Emits structured JSON annotation events for Grafana/Loki.

    Unlike numeric metrics recorded to a Prometheus ``Gauge``, an ``Annotation``
    emits a single JSON line per event to a file under the Ray session logs dir.
    On Anyscale, Vector tails that file and forwards the lines to Loki, and
    Grafana renders each one as a point annotation on a dashboard via a Loki
    annotation datasource.

    .. note::

        Annotations are positioned on the Grafana timeline by the timestamp of
        the log line, which is recorded when the event is emitted. Prometheus
        metrics, by contrast, are only observed at the scrape interval (e.g.
        every 10-15s), so a metric graph may lag the true value by up to one
        scrape period. As a result, an annotation and the metric graph it
        relates to can appear slightly out of sync on the dashboard.

    Args:
        source: Marker value written to the ``annotation_source`` field of every
            emitted event and used by LogQL to select annotation lines out of
            the log stream (e.g. ``"ray_train_annotation"``).
        base_tags: Tags attached to every emitted event (e.g. run name/id and
            world rank). These identify the run/worker so annotations can be
            filtered per run in LogQL.
    """

    _RESERVED_FIELDS = frozenset({"annotation_source", "timestamp_s", "event"})

    def __init__(
        self,
        source: str,
        base_tags: Dict[str, str],
    ):
        self._source = source
        self._base_tags = base_tags
        self._logger = self._get_logger()

        self._emit_failure_reported = False

    @staticmethod
    def _get_logger() -> logging.Logger:
        """Lazily configure and return the dedicated file-based annotation logger.

        Returns:
            The shared ``ray.annotations`` logger.
        """
        annotation_logger = logging.getLogger("ray.annotations")
        with _logger_lock:
            annotation_logger.setLevel(logging.INFO)
            # Disable propagating to the root logger echoed to the terminal.
            # Set unconditionally: if something else attached a handler to this
            # logger first, propagation must still be off so annotation JSON
            # lines never reach the terminal.
            annotation_logger.propagate = False

            if not any(
                isinstance(handler, _AnnotationFileHandler)
                for handler in annotation_logger.handlers
            ):
                handler = _AnnotationFileHandler()
                # Emit the raw message (a JSON line) with no extra formatting.
                handler.setFormatter(logging.Formatter("%(message)s"))
                annotation_logger.addHandler(handler)

        return annotation_logger

    def annotate(self, event: str, **fields: Any) -> None:
        """Emit a single annotation event as one JSON line to the annotation log file.

        Args:
            event: The event name (e.g. ``"controller_state_change"``). Used to
                filter annotations by type in LogQL.
            **fields: Arbitrary key-value pairs to include in the emitted JSON.
                Which fields an event carries (e.g. ``message``, ``severity``,
                or event-specific data) is defined by the caller and the
                dashboard queries that consume it.

        Annotations are best-effort observability and never on the critical
        path, so any failure here is swallowed rather than propagated to the
        caller.
        """
        try:
            # Drop only the colliding fields; the rest of the event (most
            # importantly its `message`) is still worth emitting.
            emitted_fields = {}
            for key, value in fields.items():
                if key in self._RESERVED_FIELDS or key in self._base_tags:
                    logger.warning(
                        "Annotation field %r collides with a reserved annotation field or "
                        "with an annotation tag and was dropped. Rename it for it to appear "
                        "in the emitted annotation.",
                        key,
                    )
                    continue
                emitted_fields[key] = value

            record = {
                "annotation_source": self._source,
                "timestamp_s": time.time(),
                "event": event,
                **emitted_fields,
                **self._base_tags,
            }
            self._logger.info(json.dumps(record, default=str))
        except Exception:
            if not self._emit_failure_reported:
                self._emit_failure_reported = True
                logger.warning(
                    "Failed to emit the %r annotation; continuing. "
                    "Further annotation failures will not be logged.",
                    event,
                    exc_info=True,
                )
