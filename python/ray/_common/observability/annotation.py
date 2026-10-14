import json
import logging
import logging.handlers
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple, Union

import numpy as np

from ray._private.protobuf_compat import message_to_dict
from ray.core.generated.annotation_event_pb2 import AnnotationEvent
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

# Each process writes `<session_dir>/logs/annotations_<pid>.log`. Log collectors
# match on this name, so changing it breaks every deployed collector config.
ANNOTATION_FILE_PREFIX = "annotations"

# Severities mirror the Python logging levels, so a caller passes the level it
# would log the same event at.
_SEVERITY_BY_LEVEL = {
    logging.DEBUG: AnnotationEvent.Severity.DEBUG,
    logging.INFO: AnnotationEvent.Severity.INFO,
    logging.WARNING: AnnotationEvent.Severity.WARNING,
    logging.ERROR: AnnotationEvent.Severity.ERROR,
    logging.CRITICAL: AnnotationEvent.Severity.CRITICAL,
}


# Log backends drop lines past a size limit (Loki defaults to 256KB), so a
# field value is truncated rather than emitted unbounded.
_MAX_FIELD_LENGTH = 4096


def _json_default(value: Any) -> Any:
    """``json.dumps`` fallback that summarizes arrays and tensors by shape only,
    since formatting their data is slow and syncs a GPU tensor to the host."""
    if isinstance(value, np.generic):
        return value.item()
    shape = getattr(value, "shape", None)
    if shape is not None:
        name = "Array" if isinstance(value, np.ndarray) else type(value).__name__
        return f"{name}(shape={tuple(shape)})"
    return str(value)


def _stringify(value: Any) -> str:
    """Render a tag or field value as a string. Non-strings are JSON encoded, so
    a bool renders as ``"true"``, the spelling a query compares against."""
    if not isinstance(value, str):
        value = json.dumps(value, default=_json_default)
    if len(value) > _MAX_FIELD_LENGTH:
        value = value[:_MAX_FIELD_LENGTH] + "...(truncated)"
    return value


def parse_severity(severity: Union[str, int]) -> "AnnotationEvent.Severity":
    """Map a Python logging level to its annotation severity.

    Args:
        severity: A logging level, as a case-insensitive name such as
            ``"warning"`` or a number such as ``logging.WARNING``.

    Returns:
        The annotation severity.

    Raises:
        ValueError: If ``severity`` is not one of the ``DEBUG``, ``INFO``,
            ``WARNING``, ``ERROR`` or ``CRITICAL`` levels.
    """
    level = severity
    if isinstance(level, str):
        # Resolves a level name to its number, or to a non-int for an unknown one.
        level = logging.getLevelName(level.upper())
    if level not in _SEVERITY_BY_LEVEL:
        raise ValueError(
            f"Invalid annotation severity {severity!r}. Expected a logging "
            "level, one of 'debug', 'info', 'warning', 'error' or 'critical', "
            "or its number such as `logging.WARNING`."
        )
    return _SEVERITY_BY_LEVEL[level]


class _RaisingRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """A ``RotatingFileHandler`` that raises write errors to its caller, rather
    than printing a traceback to stderr on every failed emit."""

    def handleError(self, record: logging.LogRecord) -> None:
        # Called from inside the handler's `except`, so this re-raises its error.
        raise


_handler_lock = threading.Lock()
# The current process's handler, with the pid and logs dir it was opened for.
_handler: Optional[Tuple[int, str, _RaisingRotatingFileHandler]] = None


def _reset_handler_lock_after_fork() -> None:
    # A thread in the parent may have held the lock at fork time, and that
    # thread does not exist in the child to release it.
    global _handler_lock
    _handler_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_handler_lock_after_fork)


def _get_handler(
    logs_dir: str, max_bytes: int, backup_count: int
) -> _RaisingRotatingFileHandler:
    """Return this process's annotation file handler for ``logs_dir``.

    The handler is replaced, and its file closed, when the logs dir changes (the
    process moved to a new Ray session) or the pid changes (the process forked,
    and the child must not write into, and rotate, its parent's file).

    Every process writes its own file, because several processes rotating one
    shared file clobber one another's backups and lose lines.
    """
    global _handler
    pid = os.getpid()
    with _handler_lock:
        if _handler is not None:
            cached_pid, cached_logs_dir, handler = _handler
            if cached_pid == pid and cached_logs_dir == logs_dir:
                return handler
            handler.close()
            _handler = None

        os.makedirs(logs_dir, exist_ok=True)
        handler = _RaisingRotatingFileHandler(
            os.path.join(logs_dir, f"{ANNOTATION_FILE_PREFIX}_{pid}.log"),
            maxBytes=max_bytes,
            backupCount=backup_count,
            # Events carry user-supplied text, which the platform default
            # encoding cannot always write under a `C`/`POSIX` locale.
            encoding="utf-8",
        )
        _handler = (pid, logs_dir, handler)
        return handler


class Annotation:
    """Emits structured annotation events for Grafana and Loki.

    Each event is an
    :class:`~ray.core.generated.annotation_event_pb2.AnnotationEvent` written as
    one JSON line to ``annotations_<pid>.log`` in the Ray session logs dir. A log
    collector ships those lines to a log backend, and Grafana renders each one as
    a point annotation. See :ref:`Overlay event annotations on the dashboards
    <grafana-dashboard-annotations>`.

    Args:
        source: Value of the ``annotation_source`` field of every event, such as
            ``"ray_train_annotation"``. Dashboard queries select on it.
        base_tags: Tags attached to every event, such as the run name, run ID
            and world rank, so that queries can filter per run or worker.
    """

    def __init__(self, source: str, base_tags: Dict[str, str]):
        self._source = source
        self._base_tags = {key: _stringify(value) for key, value in base_tags.items()}

    def annotate(
        self,
        event: str,
        *,
        message: str = "",
        severity: Union[None, str, int] = None,
        **fields: Any,
    ) -> None:
        """Emit a single annotation event to the annotation log file.

        Annotations are best-effort observability, so any failure is swallowed
        and warned about once rather than raised to the caller. Before Ray is
        initialized there is no session logs dir, so the event is dropped.

        Args:
            event: The event name, such as ``"controller_state_change"``.
            message: Human-readable description, shown as the annotation text.
            severity: A Python logging level by name or number, or ``None`` for
                an event with no notion of severity.
            **fields: Event-specific key-value pairs. Values are stringified,
                because a log backend stores them as string labels.
        """
        from ray._private.worker import _global_node

        if _global_node is None:
            return

        try:
            annotation_event = AnnotationEvent(
                annotation_source=self._source,
                event=event,
                timestamp_s=time.time(),
                # Identifies the cluster that emitted the event
                session_name=_global_node.session_name,
                message=message,
                severity=(
                    AnnotationEvent.Severity.SEVERITY_UNSPECIFIED
                    if severity is None
                    else parse_severity(severity)
                ),
                tags=self._base_tags,
                fields={key: _stringify(value) for key, value in fields.items()},
            )
            line = json.dumps(
                message_to_dict(
                    annotation_event,
                    always_print_fields_with_no_presence=True,
                    preserving_proto_field_name=True,
                    use_integers_for_enums=False,
                )
            )
            # Rotate with the same limits as the other files in the logs dir.
            handler = _get_handler(
                _global_node.get_logs_dir_path(),
                _global_node.max_bytes,
                _global_node.backup_count,
            )
            handler.handle(logging.makeLogRecord({"msg": line}))
        except Exception:
            if log_once(f"ray_annotation_failed_{self._source}_{event}"):
                logger.warning(
                    "Failed to emit the %r annotation; continuing. Further "
                    "failures for this event will not be logged.",
                    event,
                    exc_info=True,
                )
