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
