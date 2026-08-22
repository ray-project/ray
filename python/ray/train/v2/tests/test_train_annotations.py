import json
import logging
import sys
from unittest.mock import MagicMock

import pytest

import ray
import ray.train
import ray.train.v2._internal.execution.train_fn_utils
from ray.train.v2._internal.constants import (
    TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE,
    TRAIN_ANNOTATION_RAY_TRAIN_REPORT,
)
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    TrainContext,
)
from ray.train.v2.api import train_fn_utils
from ray.train.v2.tests.util import create_dummy_run_context


def create_train_context(world_rank: int = 0) -> TrainContext:
    """Build the internal worker ``TrainContext`` that ``annotation`` lives on."""
    return TrainContext(
        train_run_context=create_dummy_run_context(),
        distributed_context=DistributedContext(
            world_rank=world_rank,
            world_size=2,
            local_rank=world_rank,
            local_world_size=2,
            node_rank=0,
        ),
        # Mock everything the annotation path doesn't touch.
        execution_context=MagicMock(),
        storage_context=MagicMock(),
        preemption_context=MagicMock(),
        controller_actor=MagicMock(),
        dataset_shard_provider=MagicMock(),
    )


@pytest.fixture
def in_train_worker(monkeypatch):
    """Make the training-function-only APIs usable with a dummy train context."""
    monkeypatch.setattr(
        ray.train.v2._internal.execution.train_fn_utils,
        "get_train_fn_utils",
        MagicMock(),
    )
    monkeypatch.setattr(train_fn_utils, "get_train_fn_utils", MagicMock())
    train_context = create_train_context()
    monkeypatch.setattr(train_fn_utils, "get_train_context", lambda: train_context)
    yield train_context


@pytest.fixture
def captured_api_warnings():
    """Capture what ``ray.train``'s API module logs about itself.

    The ``ray`` logger tree does not propagate to the root logger, so pytest's
    ``caplog`` (whose handler sits on the root logger) never sees these records.
    """
    records = []

    class _CaptureHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    api_logger = logging.getLogger(train_fn_utils.__name__)
    handler = _CaptureHandler()
    handler.setLevel(logging.WARNING)
    api_logger.addHandler(handler)
    try:
        yield records
    finally:
        api_logger.removeHandler(handler)


def test_report_emits_annotation(in_train_worker, captured_annotations):
    """``ray.train.report`` annotates on every call from rank 0."""
    ray.train.report({"loss": 0.5})

    assert len(captured_annotations) == 1
    record = captured_annotations[0]
    assert record["event"] == TRAIN_ANNOTATION_RAY_TRAIN_REPORT
    assert json.loads(record["metrics"]) == {"loss": 0.5}
    assert record["has_checkpoint"] is False


def test_annotate_emits_annotation(in_train_worker, captured_annotations):
    """``ray.train.annotate`` emits a single custom annotation."""
    ray.train.annotate(message="finished epoch", epoch=3)

    assert len(captured_annotations) == 1
    record = captured_annotations[0]
    assert record["event"] == TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE
    assert record["message"] == "finished epoch"
    assert json.loads(record["fields"]) == {"epoch": 3}


def test_annotate_rejects_invalid_severity(in_train_worker, captured_annotations):
    """An invalid severity must raise rather than emit an annotation that matches
    none of the dashboard layers, which would silently never appear."""
    with pytest.raises(ValueError, match="Invalid annotation severity"):
        ray.train.annotate(message="finished epoch", severity="critical")

    assert captured_annotations == []


def test_report_annotation_failure_reported_once(
    monkeypatch, in_train_worker, captured_annotations, captured_api_warnings
):
    """The annotation is a side effect of ``ray.train.report``, so a failure in
    it must never break or block the report barrier. It also must not log a
    traceback per call, since ``ray.train.report`` can be called thousands of
    times per run."""
    monkeypatch.setattr(train_fn_utils, "_annotation_failures_reported", set())
    monkeypatch.setattr(
        in_train_worker.annotation, "annotate", MagicMock(side_effect=RuntimeError)
    )

    for _ in range(5):
        ray.train.report({"loss": 0.5})

    assert len(captured_api_warnings) == 1
    assert captured_api_warnings[0].exc_info is not None
    assert TRAIN_ANNOTATION_RAY_TRAIN_REPORT in captured_api_warnings[0].getMessage()
    assert captured_annotations == []


def test_annotate_propagates_annotation_errors(
    monkeypatch, in_train_worker, captured_annotations
):
    """Unlike ``ray.train.report``, ``ray.train.annotate`` is an explicit user
    call, so a failure in the annotation path raises rather than being silently
    swallowed. Emission and I/O failures are still handled (swallowed and warned
    once) inside ``Annotation.annotate`` itself."""
    monkeypatch.setattr(
        in_train_worker.annotation, "annotate", MagicMock(side_effect=RuntimeError)
    )

    with pytest.raises(RuntimeError):
        ray.train.annotate(message="finished epoch")

    assert captured_annotations == []


def test_annotation_no_op_without_train_context(monkeypatch, captured_api_warnings):
    """Local mode (``ScalingConfig(num_workers=0)``) never initializes the
    internal train context, which is not an annotation failure: annotating must
    be a silent no-op rather than log a warning and a traceback per run."""
    monkeypatch.setattr(
        ray.train.v2._internal.execution.train_fn_utils,
        "get_train_fn_utils",
        MagicMock(),
    )
    monkeypatch.setattr(train_fn_utils, "get_train_fn_utils", MagicMock())
    monkeypatch.setattr(train_fn_utils, "_annotation_failures_reported", set())

    def raise_no_context():
        raise RuntimeError("TrainContext has not been initialized.")

    monkeypatch.setattr(train_fn_utils, "get_train_context", raise_no_context)

    ray.train.annotate(message="finished epoch")
    ray.train.report({"loss": 0.5})

    assert captured_api_warnings == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
