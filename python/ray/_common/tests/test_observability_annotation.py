import glob
import json
import logging
import os
import sys
from unittest import mock

import numpy as np
import pytest

from ray._common.observability import annotation as annotation_mod
from ray._common.observability.annotation import Annotation
from ray.util.debug import reset_log_once

ANNOTATION_SOURCE = "test_annotation_source"
RUN_NAME_TAG_KEY = "run_name"
RUN_ID_TAG_KEY = "run_id"


class FakeNode:
    """Stands in for ``_global_node`` so emits don't need a real Ray session."""

    session_name = "session_2020-01-01_00-00-00_000000_1"
    max_bytes = 512 * 1024 * 1024
    backup_count = 5

    def __init__(self, logs_dir: str):
        self._logs_dir = logs_dir

    def get_logs_dir_path(self) -> str:
        return self._logs_dir


def read_annotations(logs_dir) -> list:
    """Return the annotation records written to ``logs_dir``, in order."""
    paths = glob.glob(os.path.join(str(logs_dir), "annotations_*.log"))
    assert len(paths) == 1, paths
    with open(paths[0], encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


@pytest.fixture(autouse=True)
def reset_annotation_handler():
    """Close the process-global annotation handler so one test's file handle
    doesn't leak into another's."""
    yield
    with annotation_mod._handler_lock:
        if annotation_mod._handler is not None:
            annotation_mod._handler.close()
            annotation_mod._handler = None


@pytest.fixture
def logs_dir(monkeypatch, tmp_path):
    """Point annotation emits at a temporary session logs dir."""
    import ray._private.worker as worker_mod

    path = tmp_path / "logs"
    monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(path)))
    yield path


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

    logger = logging.getLogger(annotation_mod.__name__)
    handler = _CaptureHandler()
    handler.setLevel(logging.WARNING)
    logger.addHandler(handler)
    reset_log_once()
    try:
        yield records
    finally:
        logger.removeHandler(handler)


def test_annotation_emits_json_line(logs_dir):
    """An annotation is one line: the ``AnnotationEvent`` serialized to a flat
    JSON object, whose field names the dashboard queries match on."""
    annotation = Annotation(
        source=ANNOTATION_SOURCE,
        base_tags={RUN_NAME_TAG_KEY: "my_run", RUN_ID_TAG_KEY: "abc123"},
    )
    annotation.annotate(
        event="custom_event",
        # Non-ASCII, which JSON escapes, so the file is ASCII in any locale.
        message="INITIALIZING → RUNNING",
        severity="warning",
        epoch=3,
        has_checkpoint=False,
    )
    annotation.annotate(event="custom_event")

    first, second = read_annotations(logs_dir)
    assert first["annotation_source"] == ANNOTATION_SOURCE
    assert first["event"] == "custom_event"
    assert first["message"] == "INITIALIZING → RUNNING"
    assert first["severity"] == "WARNING"
    assert isinstance(first["timestamp_s"], float)
    assert first["session_name"] == FakeNode.session_name
    assert first["tags"] == {RUN_NAME_TAG_KEY: "my_run", RUN_ID_TAG_KEY: "abc123"}
    # Fields are stringified, because a log backend stores them as string labels.
    assert first["fields"] == {"epoch": "3", "has_checkpoint": "false"}
    # An event with no severity leaves it unspecified rather than defaulting.
    assert second["severity"] == "SEVERITY_UNSPECIFIED"
    assert second["message"] == ""


@pytest.mark.parametrize(
    "value, expected",
    [
        # Scalars, such as a 0-d loss tensor, render as their value.
        ({"loss": np.float32(0.5)}, {"loss": 0.5}),
        ({"loss": np.array(0.5)}, {"loss": 0.5}),
        # Arrays render by shape only.
        ({"x": np.zeros((2, 3))}, {"x": "Array(shape=(2, 3))"}),
        # A key JSON can't encode is skipped, rather than dropping the event.
        ({("val", "acc"): 0.9, "loss": 0.5}, {"loss": 0.5}),
    ],
)
def test_annotation_field_encoding(logs_dir, value, expected):
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
        event="custom_event", metrics=value
    )

    (record,) = read_annotations(logs_dir)
    assert json.loads(record["fields"]["metrics"]) == expected


def test_annotation_drops_before_ray_init(monkeypatch, tmp_path):
    """Before Ray is initialized there is no session logs dir to write to, so
    emits are dropped, not raised."""
    import ray._private.worker as worker_mod

    monkeypatch.setattr(worker_mod, "_global_node", None)

    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(event="dropped")

    assert list(tmp_path.iterdir()) == []


def test_annotation_write_failure_reported_once(logs_dir, captured_warnings, capsys):
    """A failed write, such as on a full disk, is warned about once, rather than
    printing the logging module's traceback to stderr on every emit."""
    annotation = Annotation(source=ANNOTATION_SOURCE, base_tags={})

    # `RotatingFileHandler.emit` passes this error on to `handleError`.
    with mock.patch.object(
        logging.FileHandler, "emit", side_effect=OSError("No space left on device")
    ):
        for _ in range(3):
            annotation.annotate(event="custom_event")

    assert len(captured_warnings) == 1
    assert "Logging error" not in capsys.readouterr().err


@pytest.mark.skipif(not hasattr(os, "fork"), reason="Requires os.fork.")
def test_annotation_forked_process_writes_its_own_file(logs_dir):
    """Each process writes ``annotations_<pid>.log``, including a process forked
    after its parent emitted, such as a fork-based DataLoader worker, which must
    not write into, and rotate, its parent's file."""
    annotation = Annotation(source=ANNOTATION_SOURCE, base_tags={})
    annotation.annotate(event="custom_event", message="from-parent")

    pid = os.fork()
    if pid == 0:
        # Exit without running pytest's teardown in the child.
        try:
            annotation.annotate(event="custom_event", message="from-child")
        finally:
            os._exit(0)
    os.waitpid(pid, 0)

    def messages(file_pid):
        path = os.path.join(str(logs_dir), f"annotations_{file_pid}.log")
        with open(path, encoding="utf-8") as f:
            return [json.loads(line)["message"] for line in f]

    assert messages(os.getpid()) == ["from-parent"]
    assert messages(pid) == ["from-child"]


def test_annotation_rotates_with_the_node_limits(logs_dir):
    """The annotation file rotates with the same limits as the rest of the logs
    dir (``RAY_ROTATION_MAX_BYTES``, ``RAY_ROTATION_BACKUP_COUNT``)."""
    with mock.patch.object(FakeNode, "max_bytes", 1000), mock.patch.object(
        FakeNode, "backup_count", 2
    ):
        annotation = Annotation(source=ANNOTATION_SOURCE, base_tags={})
        for i in range(50):
            annotation.annotate(event="custom_event", message=f"event-{i}")

    file_name = f"annotations_{os.getpid()}.log"
    assert sorted(os.listdir(str(logs_dir))) == [
        file_name,
        f"{file_name}.1",
        f"{file_name}.2",
    ]


def test_annotation_follows_a_new_session(monkeypatch, tmp_path):
    """A process that moves to a new session, such as one that calls
    ``ray.shutdown()`` then ``ray.init()``, writes to the new session's logs dir
    and closes the previous session's file."""
    import ray._private.worker as worker_mod

    annotation = Annotation(source=ANNOTATION_SOURCE, base_tags={})
    monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(tmp_path / "a")))
    annotation.annotate(event="custom_event", message="from-a")
    handler_a = annotation_mod._handler

    monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(tmp_path / "b")))
    annotation.annotate(event="custom_event", message="from-b")

    assert annotation_mod._handler is not handler_a
    assert handler_a.stream is None
    assert [r["message"] for r in read_annotations(tmp_path / "a")] == ["from-a"]
    assert [r["message"] for r in read_annotations(tmp_path / "b")] == ["from-b"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
