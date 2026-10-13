import glob
import json
import logging
import os
import sys
from unittest import mock

import pytest

import ray
from ray._common.observability import annotation as annotation_mod
from ray._common.observability.annotation import Annotation

ANNOTATION_SOURCE = "test_annotation_source"
RUN_NAME_TAG_KEY = "run_name"
RUN_ID_TAG_KEY = "run_id"

_ANNOTATION_MODULE_LOGGER = annotation_mod.__name__


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


def reset_annotation_writer():
    """Close the process-global annotation writer so one test's file handle
    doesn't leak into another's."""
    with annotation_mod._writer_lock:
        if annotation_mod._writer is not None:
            annotation_mod._writer[2].close()
            annotation_mod._writer = None


@pytest.fixture
def logs_dir(monkeypatch, tmp_path):
    """Point annotation emits at a temporary session logs dir."""
    import ray._private.worker as worker_mod

    path = tmp_path / "logs"
    monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(path)))
    try:
        yield path
    finally:
        reset_annotation_writer()


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


def test_annotation_emits_json_line(logs_dir):
    """An annotation is one line: the ``AnnotationEvent`` serialized to a flat
    JSON object, whose field names the dashboard queries match on."""
    annotation = Annotation(
        source=ANNOTATION_SOURCE,
        base_tags={RUN_NAME_TAG_KEY: "my_run", RUN_ID_TAG_KEY: "abc123"},
    )
    annotation.annotate(
        event="custom_event", message="hello", severity="warning", epoch=3, loss=0.5
    )

    records = read_annotations(logs_dir)
    assert len(records) == 1
    data = records[0]
    assert data["annotation_source"] == ANNOTATION_SOURCE
    assert data["event"] == "custom_event"
    assert data["message"] == "hello"
    assert data["severity"] == "WARNING"
    assert isinstance(data["timestamp_s"], float)
    assert data["session_name"] == FakeNode.session_name
    assert data["tags"] == {RUN_NAME_TAG_KEY: "my_run", RUN_ID_TAG_KEY: "abc123"}
    # Fields are stringified, because a log backend stores them as string labels.
    assert data["fields"] == {"epoch": "3", "loss": "0.5"}


def test_annotation_without_severity(logs_dir):
    """An event with no notion of severity leaves it unspecified rather than
    defaulting to one, so a dashboard can tell the two apart."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(event="custom_event")

    data = read_annotations(logs_dir)[0]
    assert data["severity"] == "SEVERITY_UNSPECIFIED"
    assert data["message"] == ""


def test_annotation_unknown_severity_is_reported(logs_dir, captured_warnings):
    """An unrecognized severity is a caller bug, but not a reason to lose the
    event, so it is emitted without one and warned about."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
        event="custom_event", severity="bogus"
    )

    data = read_annotations(logs_dir)[0]
    assert data["severity"] == "SEVERITY_UNSPECIFIED"
    assert len(captured_warnings) == 1


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


def test_annotation_fields_cannot_shadow_the_schema(logs_dir):
    """``session_name`` is what isolates one cluster's annotations from
    another's, so a caller-supplied field must never be able to overwrite it.
    Fields have their own key in the schema, so a collision is not possible:
    the field is emitted, under ``fields``, and the real value is untouched."""
    annotation = Annotation(
        source=ANNOTATION_SOURCE, base_tags={RUN_NAME_TAG_KEY: "my_run"}
    )
    annotation.annotate(
        event="custom_event",
        session_name="hijacked",
        annotation_source="hijacked",
        **{RUN_NAME_TAG_KEY: "hijacked"},
    )

    data = read_annotations(logs_dir)[0]
    assert data["session_name"] == FakeNode.session_name
    assert data["annotation_source"] == ANNOTATION_SOURCE
    assert data["tags"][RUN_NAME_TAG_KEY] == "my_run"
    assert data["fields"] == {
        "session_name": "hijacked",
        "annotation_source": "hijacked",
        RUN_NAME_TAG_KEY: "hijacked",
    }


def test_annotation_writes_one_file_per_process(logs_dir):
    """Annotations are emitted by every worker process, not by a per-node
    singleton, so each process writes its own file: several processes rotating
    one file race and lose lines. Log collectors match on this name."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(event="custom_event")

    assert os.path.exists(os.path.join(str(logs_dir), f"annotations_{os.getpid()}.log"))


@pytest.mark.skipif(not hasattr(os, "fork"), reason="Requires os.fork.")
def test_annotation_forked_process_writes_its_own_file(logs_dir):
    """A process forked after its parent emitted, such as a fork-based
    DataLoader worker, must not inherit the parent's cached logger: it would
    write into, and rotate, the parent's file."""
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
    dir (``RAY_ROTATION_MAX_BYTES``, ``RAY_ROTATION_BACKUP_COUNT``), so a long
    run can't grow it without bound."""
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


def test_annotation_closes_the_previous_session_file(monkeypatch, tmp_path):
    """A process that moves to a new session keeps one writer, not one per
    session: the previous session's file is closed rather than held open for
    the life of the process."""
    import ray._private.worker as worker_mod

    annotation = Annotation(source=ANNOTATION_SOURCE, base_tags={})
    try:
        monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(tmp_path / "a")))
        annotation.annotate(event="custom_event", message="from-a")
        writer_a = annotation_mod._writer[2]

        monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(tmp_path / "b")))
        annotation.annotate(event="custom_event", message="from-b")
        writer_b = annotation_mod._writer[2]

        assert writer_b is not writer_a
        assert writer_a._handler.stream is None
        assert [r["message"] for r in read_annotations(tmp_path / "a")] == ["from-a"]
        assert [r["message"] for r in read_annotations(tmp_path / "b")] == ["from-b"]
    finally:
        reset_annotation_writer()


def test_annotation_writes_utf8(logs_dir):
    """Annotation messages can contain non-ASCII characters (e.g. Ray Train's
    controller state-change messages contain ``→``), which the platform default
    encoding cannot write under a ``C``/``POSIX`` locale."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
        event="controller_state_change", message="Controller: INITIALIZING → RUNNING"
    )

    data = read_annotations(logs_dir)[0]
    assert data["message"] == "Controller: INITIALIZING → RUNNING"


def test_annotation_drops_before_ray_init(monkeypatch, tmp_path):
    """Before Ray is initialized (``_global_node is None``) there is no session
    logs dir to write to, so emits are dropped, not raised."""
    import ray._private.worker as worker_mod

    monkeypatch.setattr(worker_mod, "_global_node", None)

    # Must not raise even though Ray isn't up yet.
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(event="dropped")

    assert list(tmp_path.iterdir()) == []


def test_annotation_follows_a_session_restart():
    """End-to-end: across a real ``ray.shutdown()`` + ``ray.init()``, an emit
    must land in the *new* session's logs dir rather than keep appending to the
    previous (now stale) session's file."""
    import ray._private.worker as worker

    ray.shutdown()
    reset_annotation_writer()

    try:
        # --- Session A ---
        ray.init(num_cpus=1, include_dashboard=False)
        logs_dir_a = worker._global_node.get_logs_dir_path()

        Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
            event="test_restart", message="from-session-a"
        )
        assert read_annotations(logs_dir_a)[0]["message"] == "from-session-a"

        ray.shutdown()

        # --- Restart into Session B ---
        ray.init(num_cpus=1, include_dashboard=False)
        logs_dir_b = worker._global_node.get_logs_dir_path()

        # A restart yields a fresh, distinct session logs dir.
        assert logs_dir_b != logs_dir_a

        Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
            event="test_restart", message="from-session-b"
        )

        # The new annotation lands in session B and not in session A.
        messages_b = [r["message"] for r in read_annotations(logs_dir_b)]
        messages_a = [r["message"] for r in read_annotations(logs_dir_a)]
        assert messages_b == ["from-session-b"]
        assert messages_a == ["from-session-a"]
    finally:
        reset_annotation_writer()
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
