import json
import logging
import os
import time
from typing import Any, Dict, Optional

_ANNOTATION_LOGGER_BASE_NAME = "ray.annotations"


class _AnnotationFileHandler(logging.Handler):
    """Write annotation records to a per-process file in the Ray session logs dir.

    The session logs directory is only known once Ray is initialized, so the
    underlying ``FileHandler`` is created lazily on the first ``emit`` after
    ``ray.init``. Records emitted before Ray is up are dropped: annotations are
    best-effort observability and never on the critical path.
    """

    def __init__(self):
        super().__init__()
        self._handler: Optional[logging.FileHandler] = None

    def emit(self, record: logging.LogRecord) -> None:
        if self._handler is None:
            from ray._private.worker import _global_node

            if _global_node is None:
                # Ray is not initialized yet; retry on the next emit.
                return

            try:
                # ``get_logs_dir_path`` returns the real timestamped session logs dir
                logs_dir = _global_node.get_logs_dir_path()
                os.makedirs(logs_dir, exist_ok=True)

                filename = f"runtime_env_annotations_{os.getpid()}.log"
                self._handler = logging.FileHandler(os.path.join(logs_dir, filename))
                self._handler.setFormatter(self.formatter)
            except Exception:
                logging.exception("Creating the Annotation logger failed")
                return

        self._handler.emit(record)


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

    def __init__(
        self,
        source: str,
        base_tags: Dict[str, str],
    ):
        self._source = source
        self._base_tags = base_tags
        self._logger = self._get_logger()

    @staticmethod
    def _get_logger() -> logging.Logger:
        """Lazily configure and return the dedicated file-based annotation logger.

        Returns:
            The shared ``ray.annotations`` logger.
        """
        logger = logging.getLogger(_ANNOTATION_LOGGER_BASE_NAME)
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            # Disable propagating to the root logger that's echoed to the terminal.
            logger.propagate = False
            handler = _AnnotationFileHandler()
            # Emit the raw message (a JSON line) with no extra formatting for Loki
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)

        return logger

    def annotate(self, event: str, **fields: Any) -> None:
        """Emit a single annotation event as one JSON line to the annotation log file.

        Args:
            event: The event name (e.g. ``"controller_state_change"``). Used to
                filter annotations by type in LogQL.
            **fields: Arbitrary key-value pairs to include in the emitted JSON.
                Which fields an event carries (e.g. ``message``, ``severity``,
                or event-specific data) is defined by the caller and the
                dashboard queries that consume it.
        """
        record = {
            "annotation_source": self._source,
            "timestamp": time.time(),
            "event": event,
            **fields,
            **self._base_tags,
        }
        self._logger.info(json.dumps(record, default=str))
