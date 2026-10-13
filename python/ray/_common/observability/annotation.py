import json
import logging
import logging.handlers
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

from ray._private.protobuf_compat import message_to_dict
from ray.core.generated.annotation_event_pb2 import AnnotationEvent

logger = logging.getLogger(__name__)

# Each process writes `<session_dir>/logs/annotations_<pid>.log`. Log collectors
# match on this name, so changing it breaks every deployed collector config.
ANNOTATION_FILE_PREFIX = "annotations"

_SEVERITY_BY_NAME = {
    "info": AnnotationEvent.Severity.INFO,
    "warning": AnnotationEvent.Severity.WARNING,
    "error": AnnotationEvent.Severity.ERROR,
}


def _stringify(value: Any) -> str:
    """Render a tag or field value as the string a log backend stores it as.

    Non-string values go through ``json.dumps`` rather than ``str``, so that a
    bool renders as ``"true"`` rather than ``"True"`` and a query can compare it
    against the JSON spelling it sees for every other Ray field.
    """
    return value if isinstance(value, str) else json.dumps(value, default=str)


def _to_severity(severity: Optional[str]) -> "AnnotationEvent.Severity":
    """Map a severity name to its enum value, defaulting to unspecified."""
    if severity is None:
        return AnnotationEvent.Severity.SEVERITY_UNSPECIFIED

    value = _SEVERITY_BY_NAME.get(severity.lower())
    if value is None:
        logger.warning(
            "Unknown annotation severity %r, expected one of %s. The annotation "
            "is emitted without one.",
            severity,
            sorted(_SEVERITY_BY_NAME),
        )
        return AnnotationEvent.Severity.SEVERITY_UNSPECIFIED
    return value


class _AnnotationFileWriter:
    """Writes annotation events as JSON lines to one process's annotation file.

    Uses a ``RotatingFileHandler`` directly rather than through a named
    ``logging.Logger``: named loggers live in the ``logging`` module's registry
    for the life of the process, whereas this writer is replaced, and its file
    closed, whenever the process moves to a new session.

    Every process writes its own file. Several processes rotating one shared
    file each rename it independently, clobbering one another's backups and
    losing lines.
    """

    def __init__(self, logs_dir: str, max_bytes: int, backup_count: int):
        os.makedirs(logs_dir, exist_ok=True)
        self.path = os.path.join(
            logs_dir, f"{ANNOTATION_FILE_PREFIX}_{os.getpid()}.log"
        )
        self._handler = logging.handlers.RotatingFileHandler(
            self.path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            # Events carry user-supplied text, which the platform default
            # encoding cannot always write under a `C`/`POSIX` locale.
            encoding="utf-8",
        )

    def send_event(self, event: AnnotationEvent) -> None:
        line = json.dumps(
            message_to_dict(
                event,
                always_print_fields_with_no_presence=True,
                preserving_proto_field_name=True,
                use_integers_for_enums=False,
            )
        )
        self._handler.handle(logging.makeLogRecord({"msg": line}))

    def close(self) -> None:
        self._handler.close()


_writer_lock = threading.Lock()
# The current process's writer, with the pid and logs dir it was opened for.
_writer: Optional[Tuple[int, str, _AnnotationFileWriter]] = None


def _reset_writer_lock_after_fork() -> None:
    # A thread in the parent may have held the lock at fork time, and that
    # thread does not exist in the child to release it.
    global _writer_lock
    _writer_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_writer_lock_after_fork)


def _get_writer(
    logs_dir: str, max_bytes: int, backup_count: int
) -> _AnnotationFileWriter:
    """Return this process's annotation writer for ``logs_dir``.

    A process holds at most one writer. It is replaced, and the previous file
    closed, when either of the two things it was opened for changes:

    * The logs dir, when a process outlives a Ray session, such as one that
      calls ``ray.shutdown()`` and then ``ray.init()``. The emit then lands in the
      new session's logs rather than the stale one, and the stale file isn't
      left open for the life of the process.
    * The pid, when a process forks after an emit, such as a fork-based
      DataLoader worker. The child then writes a file of its own rather than
      appending to, and rotating, its parent's. Closing the inherited handler
      only closes the child's copy of the file descriptor.
    """
    global _writer
    pid = os.getpid()
    with _writer_lock:
        if _writer is not None:
            cached_pid, cached_logs_dir, writer = _writer
            if cached_pid == pid and cached_logs_dir == logs_dir:
                return writer
            writer.close()
            _writer = None

        writer = _AnnotationFileWriter(logs_dir, max_bytes, backup_count)
        _writer = (pid, logs_dir, writer)
        return writer


class Annotation:
    """Emits structured annotation events for Grafana and Loki.

    Unlike numeric metrics recorded to a Prometheus ``Gauge``, an ``Annotation``
    emits a single JSON line per event to ``annotations_<pid>.log`` in the Ray
    session logs dir. A log collector tails that file and forwards the lines to
    a log backend, and Grafana renders each one as a point annotation on a
    dashboard through an annotation datasource over that backend. See
    :ref:`Overlay event annotations on the dashboards
    <grafana-dashboard-annotations>`.

    Each line is an
    :class:`~ray.core.generated.annotation_event_pb2.AnnotationEvent` serialized
    to JSON, so the proto owns the schema the dashboard queries match on.

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
            you can filter annotations per run in LogQL. They are emitted under
            their own ``tags`` key, so a tag can never shadow a schema field.
    """

    def __init__(
        self,
        source: str,
        base_tags: Dict[str, str],
    ):
        self._source = source
        self._base_tags = {key: _stringify(value) for key, value in base_tags.items()}

        self._emit_failure_reported = False

    @staticmethod
    def _get_writer() -> Optional[_AnnotationFileWriter]:
        """Return the writer to send annotations to.

        Resolved per emit rather than in ``__init__``, because the session logs
        dir is only known once Ray is initialized, and because a process can
        outlive a session and has to follow it to the next one. ``_global_node``
        is set both in processes that called ``ray.init`` and in worker processes
        (see ``default_worker.py``), so this resolves in drivers, actors and
        tasks alike.

        Returns:
            The writer, or ``None`` before Ray is initialized, in which case
            there is nowhere to write the event and it is dropped.
        """
        from ray._private.worker import _global_node

        if _global_node is None:
            return None
        # Rotate with the same limits as the other files in the logs dir.
        return _get_writer(
            _global_node.get_logs_dir_path(),
            _global_node.max_bytes,
            _global_node.backup_count,
        )

    @staticmethod
    def _get_session_name() -> str:
        from ray._private.worker import _global_node

        if _global_node is None:
            return ""
        return _global_node.session_name

    def annotate(
        self,
        event: str,
        *,
        message: str = "",
        severity: Optional[str] = None,
        **fields: Any,
    ) -> None:
        """Emit a single annotation event to the annotation log file.

        Annotations are best-effort observability and never on the critical
        path, so this method swallows any failure rather than propagating it to
        the caller.

        Args:
            event: The event name, such as ``"controller_state_change"``. LogQL
                uses it to filter annotations by type.
            message: Human-readable description of the event, which Grafana
                shows as the annotation text.
            severity: How important the event is, one of ``"info"``,
                ``"warning"`` or ``"error"``. Grafana colors annotations by
                severity. Omit it for an event that has no notion of one.
            **fields: Arbitrary key-value pairs specific to this event, such as
                the metrics of a ``ray.train.report`` call. Values are
                stringified, because a log backend stores them as string labels.
        """
        try:
            writer = self._get_writer()
            if writer is None:
                # Ray is not initialized yet, so there is no session logs dir to
                # write to. Retried on the next emit.
                return

            writer.send_event(
                AnnotationEvent(
                    annotation_source=self._source,
                    event=event,
                    timestamp_s=time.time(),
                    # Identifies the cluster that emitted the event
                    session_name=self._get_session_name(),
                    message=message,
                    severity=_to_severity(severity),
                    tags=self._base_tags,
                    fields={key: _stringify(value) for key, value in fields.items()},
                )
            )
        except Exception:
            if not self._emit_failure_reported:
                self._emit_failure_reported = True
                logger.warning(
                    "Failed to emit the %r annotation; continuing. "
                    "Further annotation failures will not be logged.",
                    event,
                    exc_info=True,
                )
