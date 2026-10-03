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


def _get_session_name() -> str:
    """Return the current Ray session name, or "" before Ray is initialized.

    ``_global_node`` is set both in processes that called ``ray.init`` and in
    worker processes (see ``default_worker.py``), so this resolves in drivers,
    actors and tasks alike. Records emitted before Ray is up are dropped by
    :class:`_AnnotationFileHandler`, so in practice a written record always
    carries a session name.
    """
    from ray._private.worker import _global_node

    if _global_node is None:
        return ""
    return _global_node.session_name


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
    """Emits structured JSON annotation events for Grafana and Loki.

    Unlike numeric metrics recorded to a Prometheus ``Gauge``, an ``Annotation``
    emits a single JSON line per event to a file under the Ray session logs dir.
    A log collector tails that file and forwards the lines to a log backend, and
    Grafana renders each one as a point annotation on a dashboard through an
    annotation datasource over that backend. See
    :ref:`Overlay event annotations on the dashboards
    <grafana-dashboard-annotations>`.

    .. note::

        Grafana positions annotations on the timeline by the timestamp of the
        log line, which Ray records when it emits the event. Prometheus, by
        contrast, only observes metrics at the scrape interval, typically every
        10 to 15 seconds, so a metric graph can lag the true value by up to one
        scrape period. An annotation and the metric graph it relates to can
        therefore appear slightly out of sync on the dashboard.

    Args:
        source: Marker value that Ray writes to the ``annotation_source`` field
            of every emitted event, such as ``"ray_train_annotation"``. LogQL
            uses it to select annotation lines out of the log stream.
        base_tags: Tags attached to every emitted event, such as the run name,
            run ID, and world rank. These identify the run and the worker, so
            you can filter annotations per run in LogQL. Keys must not collide
            with the reserved fields ``ray_annotations``, ``annotation_source``,
            ``timestamp_s``, ``event``, and ``session_name``, which they would
            silently shadow.

    Raises:
        ValueError: If ``base_tags`` contains a reserved field name.
    """

    _RESERVED_FIELDS = frozenset(
        {"ray_annotations", "annotation_source", "timestamp_s", "event", "session_name"}
    )

    def __init__(
        self,
        source: str,
        base_tags: Dict[str, str],
    ):
        # Unlike the per-emit `**fields`, which come from user code and are
        # dropped with a warning, a colliding base tag is a programming error:
        # it would shadow a reserved field on *every* emitted event.
        reserved_tags = sorted(self._RESERVED_FIELDS.intersection(base_tags))
        if reserved_tags:
            raise ValueError(
                f"Annotation base_tags contains reserved field(s) {reserved_tags}. "
                f"Reserved fields are {sorted(self._RESERVED_FIELDS)}."
            )

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

        Annotations are best-effort observability and never on the critical
        path, so this method swallows any failure rather than propagating it to
        the caller.

        Args:
            event: The event name, such as ``"controller_state_change"``. LogQL
                uses it to filter annotations by type.
            **fields: Arbitrary key-value pairs to include in the emitted JSON.
                The caller and the dashboard queries that consume the event
                define which fields it carries, such as ``message``,
                ``severity``, or event-specific data.
        """
        try:
            # Drop colliding fields
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
                # The stream-label contract with the log collector
                # Keep in sync with ``DEFAULT_ANNOTATION_STREAM_SELECTOR``
                "ray_annotations": "true",
                "annotation_source": self._source,
                "timestamp_s": time.time(),
                "event": event,
                # Identifies the cluster that emitted the event
                "session_name": _get_session_name(),
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
