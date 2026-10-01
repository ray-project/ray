import glob
import json
import logging
import os
import sys

import pytest

import ray
from ray._common.observability import annotation as annotation_mod
from ray._common.observability.annotation import Annotation
from ray._private.event import export_event_logger

ANNOTATION_SOURCE = "test_annotation_source"
RUN_NAME_TAG_KEY = "run_name"
RUN_ID_TAG_KEY = "run_id"

_ANNOTATION_MODULE_LOGGER = annotation_mod.__name__


class FakeNode:
    """Stands in for ``_global_node`` so emits don't need a real Ray session."""

    session_name = "session_2020-01-01_00-00-00_000000_1"

    def __init__(self, logs_dir: str):
        self._logs_dir = logs_dir

    def get_logs_dir_path(self) -> str:
        return self._logs_dir


def read_annotations(logs_dir) -> list:
    """Return the annotation records written to ``logs_dir``, in order."""
    paths = glob.glob(
        os.path.join(str(logs_dir), "export_events", "event_EXPORT_ANNOTATION_*.log")
    )
    assert len(paths) == 1, paths
    with open(paths[0], encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def reset_export_event_loggers():
    """Drop the process-global export event loggers so one test's file handles
    and handlers don't leak into another's."""
    with export_event_logger._export_event_logger_lock:
        for adapter in export_event_logger._export_event_logger.values():
            for handler in adapter.logger.handlers[:]:
                adapter.logger.removeHandler(handler)
                handler.close()
        export_event_logger._export_event_logger.clear()


@pytest.fixture
def logs_dir(monkeypatch, tmp_path):
    """Point annotation emits at a temporary session logs dir."""
    import ray._private.worker as worker_mod

    path = tmp_path / "logs"
    monkeypatch.setattr(worker_mod, "_global_node", FakeNode(str(path)))
    try:
        yield path
    finally:
        reset_export_event_loggers()


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


def test_annotation_emits_export_event(logs_dir):
    """An annotation is one export event line: the export envelope, with the
    annotation schema under ``event_data``."""
    annotation = Annotation(
        source=ANNOTATION_SOURCE,
        base_tags={RUN_NAME_TAG_KEY: "my_run", RUN_ID_TAG_KEY: "abc123"},
    )
    annotation.annotate(
        event="custom_event", message="hello", severity="warning", epoch=3, loss=0.5
    )

    records = read_annotations(logs_dir)
    assert len(records) == 1
    record = records[0]
    assert record["source_type"] == "EXPORT_ANNOTATION"

    data = record["event_data"]
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

    data = read_annotations(logs_dir)[0]["event_data"]
    assert data["severity"] == "SEVERITY_UNSPECIFIED"
    assert data["message"] == ""


def test_annotation_unknown_severity_is_reported(logs_dir, captured_warnings):
    """An unrecognized severity is a caller bug, but not a reason to lose the
    event, so it is emitted without one and warned about."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
        event="custom_event", severity="critical"
    )

    data = read_annotations(logs_dir)[0]["event_data"]
    assert data["severity"] == "SEVERITY_UNSPECIFIED"
    assert len(captured_warnings) == 1


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

    data = read_annotations(logs_dir)[0]["event_data"]
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
    singleton like the other export event types, so each process writes its own
    file: several processes rotating one file race and lose lines."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(event="custom_event")

    assert os.path.exists(
        os.path.join(
            str(logs_dir), "export_events", f"event_EXPORT_ANNOTATION_{os.getpid()}.log"
        )
    )


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
        path = os.path.join(
            str(logs_dir), "export_events", f"event_EXPORT_ANNOTATION_{file_pid}.log"
        )
        with open(path, encoding="utf-8") as f:
            return [json.loads(line)["event_data"]["message"] for line in f]

    assert messages(os.getpid()) == ["from-parent"]
    assert messages(pid) == ["from-child"]


@pytest.mark.parametrize(
    "env_var, value",
    [
        ("RAY_EXPORT_EVENT_MAX_FILE_SIZE_BYTES", "12345"),
        ("RAY_EXPORT_EVENT_MAX_BACKUP_COUNT", "3"),
    ],
)
def test_export_event_rotation_is_configurable(env_var, value):
    """Annotations rotate with the export event limits, which must parse as
    integers: a non-boolean value used to read as ``False``, i.e. ``0``, which
    disables rotation of the file entirely."""
    import subprocess

    output = subprocess.check_output(
        [
            sys.executable,
            "-c",
            f"from ray._private import ray_constants; print(ray_constants.{env_var})",
        ],
        env={**os.environ, env_var: value},
        text=True,
    )
    assert output.strip() == value


def test_annotation_writes_utf8(logs_dir):
    """Annotation messages can contain non-ASCII characters (e.g. Ray Train's
    controller state-change messages contain ``→``), which the platform default
    encoding cannot write under a ``C``/``POSIX`` locale."""
    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
        event="controller_state_change", message="Controller: INITIALIZING → RUNNING"
    )

    data = read_annotations(logs_dir)[0]["event_data"]
    assert data["message"] == "Controller: INITIALIZING → RUNNING"


def test_annotation_is_not_gated_on_the_export_api(monkeypatch, logs_dir):
    """Annotations ride the export event pipeline but are not part of the export
    API: a dashboard renders them without the operator enabling anything."""
    from ray._private import ray_constants

    monkeypatch.setattr(ray_constants, "RAY_ENABLE_EXPORT_API_WRITE", False)
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_EXPORT_API_WRITE_CONFIG", [])

    Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(event="custom_event")

    assert len(read_annotations(logs_dir)) == 1


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
    reset_export_event_loggers()

    try:
        # --- Session A ---
        ray.init(num_cpus=1, include_dashboard=False)
        logs_dir_a = worker._global_node.get_logs_dir_path()

        Annotation(source=ANNOTATION_SOURCE, base_tags={}).annotate(
            event="test_restart", message="from-session-a"
        )
        assert read_annotations(logs_dir_a)[0]["event_data"]["message"] == (
            "from-session-a"
        )

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
        messages_b = [r["event_data"]["message"] for r in read_annotations(logs_dir_b)]
        messages_a = [r["event_data"]["message"] for r in read_annotations(logs_dir_a)]
        assert messages_b == ["from-session-b"]
        assert messages_a == ["from-session-a"]
    finally:
        reset_export_event_loggers()
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
