import json
import logging
import sys
from unittest.mock import MagicMock

import numpy as np
import pytest

import ray
import ray.train
import ray.train.v2._internal.execution.train_fn_utils
from ray.dashboard.modules.metrics.dashboards import train_dashboard_panels
from ray.train.v2._internal import constants
from ray.train.v2._internal.constants import (
    TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE,
    TRAIN_ANNOTATION_RAY_TRAIN_REPORT,
)
from ray.train.v2._internal.execution.checkpoint.sync_actor import (
    SynchronizationBarrierResetError,
)
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    TrainContext,
)
from ray.train.v2.api import train_fn_utils
from ray.train.v2.tests.util import create_dummy_run_context


def create_train_context(world_rank: int = 0) -> TrainContext:
    """Build an internal worker ``TrainContext`` whose report skips the barrier."""
    train_context = TrainContext(
        train_run_context=create_dummy_run_context(),
        distributed_context=DistributedContext(
            world_rank=world_rank,
            world_size=2,
            local_rank=world_rank,
            local_world_size=2,
            node_rank=0,
        ),
        # Mock everything the annotation path doesn't touch.
        execution_context=MagicMock(train_context_callbacks=[]),
        storage_context=MagicMock(),
        preemption_context=MagicMock(),
        controller_actor=MagicMock(),
        dataset_shard_provider=MagicMock(),
    )
    train_context._sync_checkpoint_dir_name_across_ranks = MagicMock(
        return_value="checkpoint_dir"
    )
    return train_context


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


def test_report_annotation(captured_annotations):
    """Rank 0 annotates each report with every metric, summarizing arrays by
    shape so that a large one is never formatted."""
    create_train_context().report(
        {
            "loss": np.float32(0.5),
            "embeddings": np.zeros((1000, 64), dtype=np.float32),
        }
    )

    (record,) = captured_annotations
    assert record["event"] == TRAIN_ANNOTATION_RAY_TRAIN_REPORT
    assert json.loads(record["fields"]["metrics"]) == {
        "loss": 0.5,
        "embeddings": "Array(shape=(1000, 64))",
    }
    assert record["fields"]["has_checkpoint"] == "false"
    assert record["fields"]["validation"] == "false"


def test_report_annotates_after_the_barrier(captured_annotations):
    """The annotation marks the moment every rank reached the report."""
    train_context = create_train_context()
    train_context._sync_checkpoint_dir_name_across_ranks.side_effect = (
        lambda _: captured_annotations.append("barrier")
    )

    train_context.report({"loss": 0.5})

    assert captured_annotations[0] == "barrier"
    assert captured_annotations[1]["event"] == TRAIN_ANNOTATION_RAY_TRAIN_REPORT


def test_report_skipped_at_the_barrier_is_not_annotated(captured_annotations):
    """A report skipped because a worker failed never happened, so it must not
    show up on the dashboard."""
    train_context = create_train_context()
    train_context._sync_checkpoint_dir_name_across_ranks.side_effect = (
        ray.exceptions.RayTaskError(
            "broadcast_from_rank_zero", "", SynchronizationBarrierResetError()
        )
    )

    train_context.report({"loss": 0.5})

    assert captured_annotations == []


def test_report_annotates_on_rank_zero_only(captured_annotations):
    create_train_context(world_rank=1).report({"loss": 0.5})

    assert captured_annotations == []


def test_annotate_emits_annotation(in_train_worker, captured_annotations):
    """``ray.train.annotate`` emits a single custom annotation."""
    ray.train.annotate(message="finished epoch", epoch=3)

    (record,) = captured_annotations
    assert record["event"] == TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE
    assert record["message"] == "finished epoch"
    assert record["severity"] == "INFO"
    assert json.loads(record["fields"]["custom_fields"]) == {"epoch": 3}


@pytest.mark.parametrize(
    "severity, expected",
    [
        ("debug", "DEBUG"),
        ("WARNING", "WARNING"),
        ("warn", "WARNING"),
        (logging.ERROR, "ERROR"),
        ("critical", "CRITICAL"),
    ],
)
def test_annotate_accepts_logging_levels(
    in_train_worker, captured_annotations, severity, expected
):
    """The severity is a Python logging level, by name in any case or by number."""
    ray.train.annotate(message="finished epoch", severity=severity)

    assert [record["severity"] for record in captured_annotations] == [expected]


@pytest.mark.parametrize("severity", ["bogus", "notset", 35])
def test_annotate_rejects_invalid_severity(
    in_train_worker, captured_annotations, severity
):
    """An invalid severity must raise rather than emit an annotation that matches
    none of the dashboard layers, which would silently never appear."""
    with pytest.raises(ValueError, match="Invalid annotation severity"):
        ray.train.annotate(message="finished epoch", severity=severity)

    assert captured_annotations == []


def test_annotate_no_op_without_train_context(monkeypatch, captured_annotations):
    """Local mode (``ScalingConfig(num_workers=0)``) never initializes the
    internal train context, so annotating is a silent no-op."""
    monkeypatch.setattr(
        ray.train.v2._internal.execution.train_fn_utils,
        "get_train_fn_utils",
        MagicMock(),
    )

    def raise_no_context():
        raise RuntimeError("TrainContext has not been initialized.")

    monkeypatch.setattr(train_fn_utils, "get_train_context", raise_no_context)

    ray.train.annotate(message="finished epoch")

    assert captured_annotations == []


@pytest.mark.parametrize(
    "name",
    [
        "TRAIN_ANNOTATION_SOURCE",
        "TRAIN_ANNOTATION_CONTROLLER_STATE_CHANGE",
        "TRAIN_ANNOTATION_RAY_TRAIN_REPORT",
        "TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE",
    ],
)
def test_dashboard_matches_emitted_annotations(name):
    """The dashboard duplicates these rather than import Ray Train, and a drift
    would leave its annotation layers silently empty."""
    assert getattr(train_dashboard_panels, name) == getattr(constants, name)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
