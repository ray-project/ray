"""Unit tests of the streaming executor side of Ray Data lineage reconstruction.
"""

import pickle
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.interfaces.ref_bundle import ReconstructionStamp
from ray.data._internal.execution.lineage_tracker import LineageTracker
from ray.data._internal.execution.streaming_executor_state import (
    _reconstruct_lost_object,
)
from ray.data.exceptions import LineageReconstructionError
from ray.exceptions import ObjectLostError


def _lost_error() -> ObjectLostError:
    return ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")


# ---------------------------------------------------------------------------
# Aborting the lost task
# ---------------------------------------------------------------------------


def _recording_done_callback(done_calls):
    """A done-callback that records the error and abort flag of each call."""

    def task_done_callback(exc, worker_stats, driver_stats, aborted):
        done_calls.append({"error": exc, "aborted_for_reconstruction": aborted})

    return task_done_callback


def _abortable_task(task_done_callback):
    """A ``DataOpTask`` in its freshly-submitted (ACTIVE) state.

    ``mark_aborted`` reads neither the generator nor the ref counter, so both are
    stubs; this keeps the test on the state machine and off a live cluster.
    """
    return DataOpTask(
        0,
        MagicMock(),  # streaming_gen
        MagicMock(),  # block_ref_counter
        "test_op",
        task_done_callback=task_done_callback,
        lineage_task_id="seed:0",
    )


def test_mark_aborted_fires_the_done_callback_exactly_once():
    """Abort from ACTIVE runs the done-callback with the error and finishes the task.

    A repeat abort, or an abort after normal completion, must not run it again: that
    would double-release the task's resource reservations.
    """
    done_calls = []
    task = _abortable_task(_recording_done_callback(done_calls))
    error = _lost_error()

    task.mark_aborted(error)
    task.mark_aborted(error)
    assert done_calls == [{"error": error, "aborted_for_reconstruction": False}]
    assert task.has_finished

    done_calls.clear()
    task = _abortable_task(_recording_done_callback(done_calls))
    task.mark_done()
    task.mark_aborted(error)
    # Only the normal completion fired, with no error.
    assert done_calls == [{"error": None, "aborted_for_reconstruction": False}]


def test_aborted_drained_task_runs_done_callback_once():
    """``_fire_done_callbacks`` must skip a drained task that was since aborted."""
    from ray.data._internal.execution.metadata_fetcher import ThreadedMetadataFetcher

    done_calls = []
    task = _abortable_task(_recording_done_callback(done_calls))
    fetcher = ThreadedMetadataFetcher()
    fetcher._drained_tasks.add(task)
    error = _lost_error()
    task.mark_aborted(error)

    fetcher._fire_done_callbacks()

    assert done_calls == [{"error": error, "aborted_for_reconstruction": False}]
    assert task not in fetcher._drained_tasks


def test_started_reconstruction_aborts_for_reconstruction():
    """A task whose lost output is being reconstructed is aborted with
    ``aborted_for_reconstruction=True``, keeping the lost-object error."""
    done_calls = []
    task = _abortable_task(_recording_done_callback(done_calls))
    tracker = MagicMock()
    # A plan with no seeds to resubmit. The test only checks how the lost task is
    # aborted.
    tracker.register_task_failed.return_value = (set(), "plan_a")
    lost_error = _lost_error()

    _reconstruct_lost_object({}, tracker, MagicMock(), task, lost_error)

    assert task.has_finished
    assert done_calls == [{"error": lost_error, "aborted_for_reconstruction": True}]


# ---------------------------------------------------------------------------
# When lineage reconstruction can't start
# ---------------------------------------------------------------------------


def test_reconstruction_error_wraps_object_lost_error():
    """An ``ObjectLostError`` triggers a lineage reconstruction attempt that fails.

    Test that the LineageReconstructionError carries both the original ObjectLostError
    and the reason for the reconstruction failure.
    """
    object_lost_error = ObjectLostError(
        ray.ObjectRef.nil().hex(), b"owner", "injected by test"
    )
    lost_output_task = _abortable_task(
        lambda exc, worker_stats, driver_stats, aborted: None
    )
    op_state = MagicMock()
    op_state.op.name = "test_op"

    # A fresh tracker has never seen the task, so reconstruction cannot start.
    with pytest.raises(LineageReconstructionError) as raised:
        _reconstruct_lost_object(
            {}, LineageTracker(), op_state, lost_output_task, object_lost_error
        )
    reconstruction_error = raised.value

    assert isinstance(raised.value, ObjectLostError)
    assert reconstruction_error.lost_error is object_lost_error
    assert reconstruction_error.__cause__ is object_lost_error
    for field in ("object_ref_hex", "owner_address", "call_site"):
        assert getattr(reconstruction_error, field) == getattr(object_lost_error, field)
    assert str(object_lost_error) in str(reconstruction_error)
    assert "not registered with the lineage graph" in str(reconstruction_error)

    unpickled_error = pickle.loads(pickle.dumps(reconstruction_error))
    assert type(unpickled_error.lost_error) is type(object_lost_error)
    assert str(unpickled_error) == str(reconstruction_error)


# ---------------------------------------------------------------------------
# Reconstruction stamps
# ---------------------------------------------------------------------------


def test_reconstruction_stamp_survives_schema_divergence():
    """``OpState.add_output`` rebuilds a bundle whose schema diverges from the
    operator's, field by field. The stamp must survive the rebuild, or the
    re-execution is bundled and submitted as a fresh task.
    """
    import pyarrow as pa

    from ray.data._internal.execution.interfaces import RefBundle
    from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry
    from ray.data._internal.execution.streaming_executor_state import (
        dedupe_schemas_with_validation,
    )
    from ray.data.block import BlockMetadata

    stamp = ReconstructionStamp(
        lineage_task_id="child:0",
        reconstruction_plan_ids=frozenset({"plan_a"}),
    )
    meta = BlockMetadata(num_rows=2, size_bytes=16, exec_stats=None, input_files=None)
    # The same column comes back all nulls on the re-run, so it is inferred as null.
    bundle = RefBundle(
        blocks=(
            BlockEntry(
                ray.ObjectRef.nil(),  # pyrefly: ignore[bad-argument-type]
                meta,
            ),
        ),
        schema=pa.schema([("x", pa.null())]),
        owns_blocks=True,
        reconstruction_stamp=stamp,
    )

    rebuilt, diverged = dedupe_schemas_with_validation(
        pa.schema([("x", pa.int64())]), bundle, enforce_schemas=False
    )

    assert diverged and rebuilt is not bundle
    assert rebuilt.reconstruction_stamp == stamp


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
