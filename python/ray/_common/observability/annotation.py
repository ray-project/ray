import json
import logging
import time
from typing import Any, Dict, Optional

from ray._private.event.export_event_logger import (
    EventLogType,
    ExportEventLoggerAdapter,
    get_export_event_logger,
)
from ray.core.generated.export_annotation_event_pb2 import ExportAnnotationEventData

logger = logging.getLogger(__name__)

_SEVERITY_BY_NAME = {
    "info": ExportAnnotationEventData.Severity.INFO,
    "warning": ExportAnnotationEventData.Severity.WARNING,
    "error": ExportAnnotationEventData.Severity.ERROR,
}


def _stringify(value: Any) -> str:
    """Render a tag or field value as the string a log backend stores it as.

    Non-string values go through ``json.dumps`` rather than ``str``, so that a
    bool renders as ``"true"`` rather than ``"True"`` and a query can compare it
    against the JSON spelling it sees for every other Ray field.
    """
    return value if isinstance(value, str) else json.dumps(value, default=str)


def _to_severity(severity: Optional[str]) -> "ExportAnnotationEventData.Severity":
    """Map a severity name to its enum value, defaulting to unspecified."""
    if severity is None:
        return ExportAnnotationEventData.Severity.SEVERITY_UNSPECIFIED

    value = _SEVERITY_BY_NAME.get(severity.lower())
    if value is None:
        logger.warning(
            "Unknown annotation severity %r, expected one of %s. The annotation "
            "is emitted without one.",
            severity,
            sorted(_SEVERITY_BY_NAME),
        )
        return ExportAnnotationEventData.Severity.SEVERITY_UNSPECIFIED
    return value


class Annotation:
    """Emits structured annotation events for Grafana and Loki.

    Unlike numeric metrics recorded to a Prometheus ``Gauge``, an ``Annotation``
    emits a single JSON line per event to a file under the Ray session logs dir.
    A log collector tails that file and forwards the lines to a log backend, and
    Grafana renders each one as a point annotation on a dashboard through an
    annotation datasource over that backend. See
    :ref:`Overlay event annotations on the dashboards
    <grafana-dashboard-annotations>`.

    Events are written through the export event pipeline as
    :class:`~ray.core.generated.export_annotation_event_pb2.ExportAnnotationEventData`,
    which owns the schema, so the emitted line is the export event envelope with
    the annotation under its ``event_data`` key. Unlike the rest of that
    pipeline, annotations are not gated on ``RAY_enable_export_api_write``: a
    dashboard renders them out of the box.

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
            their own ``tags`` key, so a tag can never shadow an envelope field.
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
    def _get_logger() -> Optional[ExportEventLoggerAdapter]:
        """Return the export event logger to write annotations to.

        Resolved per emit rather than in ``__init__``, because the session logs
        dir is only known once Ray is initialized, and because a process can
        outlive a session and has to follow it to the next one. ``_global_node``
        is set both in processes that called ``ray.init`` and in worker processes
        (see ``default_worker.py``), so this resolves in drivers, actors and
        tasks alike.

        Returns:
            The logger, or ``None`` before Ray is initialized, in which case
            there is nowhere to write the event and it is dropped.
        """
        from ray._private.worker import _global_node

        if _global_node is None:
            return None
        return get_export_event_logger(
            EventLogType.ANNOTATION, _global_node.get_logs_dir_path()
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
            export_event_logger = self._get_logger()
            if export_event_logger is None:
                # Ray is not initialized yet, so there is no session logs dir to
                # write to. Retried on the next emit.
                return

            export_event_logger.send_event(
                ExportAnnotationEventData(
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
