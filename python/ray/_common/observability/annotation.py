import json
import logging
import logging.handlers
import os
import threading
import time
from typing import Any, Dict, Optional

from ray._common.utils import env_bool
from ray._private.protobuf_compat import message_to_dict
from ray.core.generated.annotation_event_pb2 import AnnotationEvent
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

# Annotations are only useful with a log collector and log backend that Ray
# doesn't provision, so they're off by default
ANNOTATIONS_ENABLED_ENV_VAR = "RAY_ANNOTATIONS_ENABLED"


def annotations_enabled() -> bool:
    """Whether ``ANNOTATIONS_ENABLED_ENV_VAR`` turns annotations on."""
    return env_bool(ANNOTATIONS_ENABLED_ENV_VAR, False)


# Log backends drop lines past a size limit (Loki defaults to 256KB), so a
# field value is truncated rather than emitted unbounded.
_MAX_FIELD_LENGTH = 4096


def _summarize_arrays(value: Any) -> Any:
    """``json.dumps`` fallback that summarizes arrays and tensors by shape only,
    since formatting their data is slow and can produce massive text strings.

    A scalar, such as a numpy scalar or a 0-d loss tensor, is rendered as its
    value instead.
    """
    shape = getattr(value, "shape", None)
    if shape is None:
        return str(value)
    shape = tuple(shape)
    if shape == () and hasattr(value, "item"):
        return value.item()
    name = type(value).__name__
    return f"{'Array' if name == 'ndarray' else name}(shape={shape})"


def _format_field_value(value: Any) -> str:
    """Render a tag or field value as a string. Non-strings are JSON encoded, so
    a bool renders as ``"true"``, the spelling a query compares against."""
    if not isinstance(value, str):
        # `default` only applies to values, so a dict key JSON can't encode, such
        # as a tuple, is skipped rather than failing the whole event.
        value = json.dumps(value, default=_summarize_arrays, skipkeys=True)
    if len(value) > _MAX_FIELD_LENGTH:
        value = value[:_MAX_FIELD_LENGTH] + "...(truncated)"
    return value


class _RaisingRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """A ``RotatingFileHandler`` that raises write errors to its caller, rather
    than printing a traceback to stderr on every failed emit."""

    def handleError(self, record: logging.LogRecord) -> None:
        # Called from inside the handler's `except`, so this re-raises its error.
        raise


_handler_lock = threading.Lock()
_handler: Optional[_RaisingRotatingFileHandler] = None

if hasattr(os, "register_at_fork"):
    # A thread in the parent may have held the lock at fork time, and that
    # thread does not exist in the child to release it.
    os.register_at_fork(after_in_child=_handler_lock._at_fork_reinit)


def _write_annotation_line(
    line: str, logs_dir: str, max_bytes: int, backup_count: int
) -> None:
    """Write ``line`` to this process's annotation file in ``logs_dir``.

    The handler is replaced, and its file closed, when the logs dir changes (the
    process moved to a new Ray session) or the pid changes (the process forked,
    and the child must not write into, and rotate, its parent's file). The write
    happens under the same lock, so no thread writes to a handler another thread
    has closed.

    Every process writes its own file, because several processes rotating one
    shared file clobber one another's backups and lose lines.
    """
    global _handler
    # Log collectors match on this file name, so changing it breaks every
    # deployed collector config.
    path = os.path.abspath(os.path.join(logs_dir, f"annotations_{os.getpid()}.log"))
    with _handler_lock:
        if _handler is not None and _handler.baseFilename != path:
            _handler.close()
            _handler = None

        if _handler is None:
            os.makedirs(logs_dir, exist_ok=True)
            _handler = _RaisingRotatingFileHandler(
                path, maxBytes=max_bytes, backupCount=backup_count
            )

        _handler.handle(logging.makeLogRecord({"msg": line}))


class AnnotationEmitter:
    """Emits structured annotation events for dashboards to overlay.

    Each event is an
    :class:`~ray.core.generated.annotation_event_pb2.AnnotationEvent` written as
    one JSON line to ``annotations_<pid>.log`` in the Ray session logs dir. A log
    collector ships those lines to a log backend, such as Loki, and Grafana renders
    each one as a point annotation. See :ref:`Overlay event annotations on the dashboards
    <grafana-dashboard-annotations>`.

    Args:
        source: Value of the ``annotation_source`` field of every event, such as
            ``"ray_train_annotation"``. Dashboard queries select on it.
        base_tags: Tags attached to every event, such as the run name, run ID
            and world rank, so that queries can filter per run or worker.
    """

    def __init__(self, source: str, base_tags: Dict[str, str]):
        self._source = source
        self._base_tags = {
            key: _format_field_value(value) for key, value in base_tags.items()
        }

    def emit(
        self,
        event: str,
        *,
        message: str = "",
        **fields: Any,
    ) -> None:
        """Emit a single annotation event to the annotation log file.

        Annotations are best-effort observability, so any failure to write the
        event is swallowed and warned about once rather than raised to the
        caller.

        Unless ``RAY_ANNOTATIONS_ENABLED`` is set, the event is dropped. Before Ray is
        initialized there is no session logs dir, so the event is dropped.

        Args:
            event: The event name, such as ``"controller_state_change"``.
            message: Human-readable description, shown as the annotation text.
            **fields: Event-specific key-value pairs. Values are stringified,
                because a log backend stores them as string labels.
        """
        if not annotations_enabled():
            return

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
                tags=self._base_tags,
                fields={
                    key: _format_field_value(value) for key, value in fields.items()
                },
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
            _write_annotation_line(
                line,
                _global_node.get_logs_dir_path(),
                _global_node.max_bytes,
                _global_node.backup_count,
            )
        except Exception:
            if log_once(f"ray_annotation_failed_{self._source}_{event}"):
                logger.warning(
                    "Failed to emit the %r annotation; continuing. Further "
                    "failures for this event will not be logged.",
                    event,
                    exc_info=True,
                )
