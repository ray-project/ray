import json
import logging
import logging.handlers
import os
import sys

import pytest

import ray
from ray._common.observability import annotation as annotation_mod
from ray._common.observability.annotation import (
    Annotation,
    _AnnotationFileHandler,
)

# The annotation module is core and has no dependency on Ray Train, so this test
# must not import ``ray.train``
TRAIN_ANNOTATION_SOURCE = "ray_train_annotation"
RUN_NAME_TAG_KEY = "ray_train_run_name"
RUN_ID_TAG_KEY = "ray_train_run_id"

_ANNOTATION_MODULE_LOGGER = annotation_mod.__name__
_ANNOTATION_LOGGER_BASE_NAME = "ray.annotations"


def make_log_record(message: str) -> logging.LogRecord:
    """Build a minimal record to feed directly to ``_AnnotationFileHandler``."""
    return logging.LogRecord(
        name="ray.annotations",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )


@pytest.fixture
def captured_annotations():
    """Capture the annotation logger and yield the records."""
    records = []

    class _CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(json.loads(record.getMessage()))

    logger = logging.getLogger(_ANNOTATION_LOGGER_BASE_NAME)
    handler = _CaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


@pytest.fixture
def captured_warnings():
    """Capture the warnings the annotation module logs about itself.

    The ``ray`` logger tree does not propagate to the root logger, so pytest's
    ``caplog`` (whose handler sits on the root logger) never sees these records.
    """
    records = []

    class _CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logger = logging.getLogger(_ANNOTATION_MODULE_LOGGER)
    handler = _CaptureHandler()
    handler.setLevel(logging.WARNING)
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


def test_annotation_emits_json(captured_annotations):
    annotation = Annotation(
        source=TRAIN_ANNOTATION_SOURCE,
        base_tags={RUN_NAME_TAG_KEY: "my_run", RUN_ID_TAG_KEY: "abc123"},
    )
    annotation.annotate(event="custom_event", epoch=3, loss=0.5)

    assert len(captured_annotations) == 1
    record = captured_annotations[0]
    assert record["annotation_source"] == TRAIN_ANNOTATION_SOURCE
    assert record["event"] == "custom_event"

    assert "severity" not in record
    assert record[RUN_NAME_TAG_KEY] == "my_run"
    assert record[RUN_ID_TAG_KEY] == "abc123"
    assert record["epoch"] == 3
    assert record["loss"] == 0.5
    assert isinstance(record["timestamp_s"], float)
    # Empty here because Ray is not initialized; see
    # ``test_annotation_records_session_name``.
    assert record["session_name"] == ""


def test_annotation_records_session_name(monkeypatch, captured_annotations):
    """Dashboards scope annotations to a cluster with this field, so that a log
    backend aggregating several clusters cannot leak one cluster's annotations
    onto another cluster's dashboard."""

    class _FakeNode:
        session_name = "session_2020-01-01_00-00-00_000000_1"

    monkeypatch.setattr(ray._private.worker, "_global_node", _FakeNode())

    annotation = Annotation(source=TRAIN_ANNOTATION_SOURCE, base_tags={})
    annotation.annotate(event="custom_event")

    assert captured_annotations[0]["session_name"] == _FakeNode.session_name


def test_annotation_cannot_spoof_session_name(captured_annotations, captured_warnings):
    """``session_name`` is what isolates one cluster's annotations from another's,
    so a caller-supplied field must never be able to overwrite it."""
    annotation = Annotation(source=TRAIN_ANNOTATION_SOURCE, base_tags={})
    annotation.annotate(event="custom_event", session_name="hijacked")

    assert captured_annotations[0]["session_name"] != "hijacked"
    assert len(captured_warnings) == 1


def test_annotation_field_collision_drops_only_the_colliding_field(
    captured_annotations, captured_warnings
):
    """A field that collides with a reserved field or a base tag is dropped on its
    own; the rest of the event, in particular its ``message``, is still emitted."""
    annotation = Annotation(
        source=TRAIN_ANNOTATION_SOURCE,
        base_tags={RUN_NAME_TAG_KEY: "my_run"},
    )
    annotation.annotate(
        event="custom_event",
        message="still emitted",
        epoch=3,
        # Collides with a reserved field and with a base tag respectively.
        annotation_source="hijacked",
        **{RUN_NAME_TAG_KEY: "hijacked"},
    )

    assert len(captured_annotations) == 1
    record = captured_annotations[0]
    assert record["message"] == "still emitted"
    assert record["epoch"] == 3
    # The colliding fields did not overwrite the real values.
    assert record["annotation_source"] == TRAIN_ANNOTATION_SOURCE
    assert record[RUN_NAME_TAG_KEY] == "my_run"

    assert len(captured_warnings) == 2


def test_annotation_logger_setup_with_preexisting_handler():
    """Another handler on ``ray.annotations`` must not stop Ray from installing its
    file handler or from disabling propagation, which would otherwise echo every
    annotation JSON line to the terminal."""
    annotation_logger = logging.getLogger(_ANNOTATION_LOGGER_BASE_NAME)
    reset_annotation_logger()
    annotation_logger.propagate = True
    other_handler = logging.Handler()
    annotation_logger.addHandler(other_handler)

    try:
        Annotation(source=TRAIN_ANNOTATION_SOURCE, base_tags={})

        assert annotation_logger.propagate is False
        assert any(
            isinstance(handler, _AnnotationFileHandler)
            for handler in annotation_logger.handlers
        )
    finally:
        annotation_logger.removeHandler(other_handler)
        reset_annotation_logger()


class FakeNode:
    """Stands in for ``_global_node`` so emits don't need a real Ray session."""

    def __init__(self, logs_dir: str):
        self._logs_dir = logs_dir

    def get_logs_dir_path(self) -> str:
        return self._logs_dir


def test_annotation_file_handler_writes_utf8(monkeypatch, tmp_path):
    """Annotation messages contain non-ASCII characters (e.g. the ``→`` in the
    controller state-change messages), which the platform default encoding cannot
    write under a ``C``/``POSIX`` locale."""
    import ray._private.worker as worker_mod

    logs_dir = tmp_path / "logs"
    monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(logs_dir)))

    handler = _AnnotationFileHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    try:
        handler.emit(make_log_record("Controller: INITIALIZING → RUNNING"))
        handler.flush()

        assert handler._handler.encoding == "utf-8"
        log_file = logs_dir / f"annotations_{os.getpid()}.log"
        assert "INITIALIZING → RUNNING" in log_file.read_text(encoding="utf-8")
    finally:
        handler.close()


def test_annotation_file_handler_drops_before_ray_init(monkeypatch, tmp_path):
    """Before Ray is initialized (``_global_node is None``) emits are dropped,
    not raised, and no file is created."""
    import ray._private.worker as worker_mod

    monkeypatch.setattr(worker_mod, "_global_node", None)

    handler = _AnnotationFileHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    # Must not raise even though Ray isn't up yet.
    handler.emit(make_log_record("dropped"))

    assert handler._handler is None
    assert list(tmp_path.iterdir()) == []


def reset_annotation_logger():
    """Remove any process-global annotation file handler so this test's handler
    state doesn't leak into other tests sharing the ``ray.annotations`` logger."""
    logger = logging.getLogger(_ANNOTATION_LOGGER_BASE_NAME)
    for h in list(logger.handlers):
        if isinstance(h, _AnnotationFileHandler):
            logger.removeHandler(h)
            h.close()


def test_annotation_file_handler_reopens_after_session_restart():
    """End-to-end: across a real ``ray.shutdown()`` + ``ray.init()``, the
    process-global annotation handler must write into the *new* session's logs
    dir rather than keep appending to the previous (now stale) session's file.
    """
    import ray._private.worker as worker

    ray.shutdown()
    reset_annotation_logger()

    try:
        # --- Session A ---
        ray.init(num_cpus=1, include_dashboard=False)
        logs_dir_a = worker._global_node.get_logs_dir_path()

        Annotation(source=TRAIN_ANNOTATION_SOURCE, base_tags={}).annotate(
            event="test_restart", message="from-session-a"
        )
        logging.getLogger(_ANNOTATION_LOGGER_BASE_NAME).handlers[0].flush()

        file_a = os.path.join(logs_dir_a, f"annotations_{os.getpid()}.log")
        assert os.path.exists(file_a)
        assert "from-session-a" in open(file_a).read()

        ray.shutdown()

        # --- Restart into Session B ---
        ray.init(num_cpus=1, include_dashboard=False)
        logs_dir_b = worker._global_node.get_logs_dir_path()

        # A restart yields a fresh, distinct session logs dir.
        assert logs_dir_b != logs_dir_a

        Annotation(source=TRAIN_ANNOTATION_SOURCE, base_tags={}).annotate(
            event="test_restart", message="from-session-b"
        )
        logging.getLogger(_ANNOTATION_LOGGER_BASE_NAME).handlers[0].flush()

        file_b = os.path.join(logs_dir_b, f"annotations_{os.getpid()}.log")
        # The new annotation lands in session B and not in session A
        assert os.path.exists(file_b)
        assert "from-session-b" in open(file_b).read()
        assert "from-session-b" not in open(file_a).read()
    finally:
        reset_annotation_logger()
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
