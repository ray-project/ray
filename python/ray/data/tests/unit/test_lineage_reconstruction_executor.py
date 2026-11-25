"""Unit tests of the streaming executor side of Ray Data lineage reconstruction.
"""

import pickle
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.interfaces.ref_bundle import ReconstructionStamp
from ray.data._internal.execution.lineage_tracker import (
    LineageTracker,
    make_lineage_task_id,
    parse_lineage_task_id,
)
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


def _done_call(error, aborted_for_reconstruction=False):
    """What ``_recording_done_callback`` records for one done-callback call."""
    return {"error": error, "aborted_for_reconstruction": aborted_for_reconstruction}


def _recording_done_callback(done_calls):
    """A done-callback that records the error and abort flag of each call."""

    def task_done_callback(exc, worker_stats, driver_stats, aborted):
        done_calls.append(_done_call(exc, aborted))

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
    assert done_calls == [_done_call(error)]
    assert task.has_finished

    # A repeat abort doesn't run the done-callback again.
    task.mark_aborted(error)
    assert done_calls == [_done_call(error)]

    done_calls.clear()
    task = _abortable_task(_recording_done_callback(done_calls))
    task.mark_done()
    task.mark_aborted(error)
    # Only the normal completion fired, with no error.
    assert done_calls == [_done_call(None)]


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

    assert done_calls == [_done_call(error)]
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
    assert done_calls == [_done_call(lost_error, aborted_for_reconstruction=True)]


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


def _map_operator():
    """A real ``MapOperator`` that never runs, for checking its bookkeeping."""
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )
    from ray.data._internal.execution.operators.map_operator import MapOperator
    from ray.data._internal.execution.operators.map_transformer import (
        BlockMapTransformFn,
        MapTransformer,
    )
    from ray.data.context import DataContext

    data_context = DataContext.get_current()
    return MapOperator.create(
        MapTransformer([BlockMapTransformFn(lambda blocks, ctx: blocks)]),
        InputDataBuffer(data_context, []),
        data_context,
        name="Op",
    )


def test_lineage_task_id_round_trips():
    task_id = make_lineage_task_id("op", 51)
    assert task_id == "op:51"
    assert parse_lineage_task_id(task_id) == ("op", 51)


@pytest.mark.parametrize("task_id", ["op", "", ":3", "op:", "op:x", "op:-1", "op:1.5"])
def test_malformed_lineage_task_id_is_rejected(task_id):
    with pytest.raises(ValueError, match="Malformed lineage task ID"):
        parse_lineage_task_id(task_id)


def test_operator_only_reads_task_indices_from_its_own_lineage_task_ids():
    """A malformed ID, including the bare operator ID, belongs to no operator."""
    op = _map_operator()

    assert op.owns_data_task(op._lineage_task_id_for(3))
    assert op._task_index_of(op._lineage_task_id_for(3)) == 3
    for not_ours in [op.id, make_lineage_task_id("other-op", 3)]:
        assert not op.owns_data_task(not_ours)
        with pytest.raises(ValueError):
            op._task_index_of(not_ours)


def test_reconstruction_task_context_reuses_the_original_task_index():
    """Output file names, seeds and ``monotonically_increasing_id`` derive from
    ``TaskContext.task_idx``. A reconstruction task must see the index of the task
    it re-runs, or it writes a second file or draws different values. The operator
    keeps its own counter for bookkeeping."""
    from dataclasses import replace

    from ray.data._internal.execution.interfaces import RefBundle
    from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry
    from ray.data.block import BlockMetadata

    op = _map_operator()
    op._next_data_task_idx = 7
    meta = BlockMetadata(num_rows=1, size_bytes=8, exec_stats=None, input_files=None)
    fresh = RefBundle(
        blocks=(
            BlockEntry(
                ray.ObjectRef.nil(),  # pyrefly: ignore[bad-argument-type]
                meta,
            ),
        ),
        schema=None,
        owns_blocks=True,
    )
    rerun_of_task_2 = replace(
        fresh,
        reconstruction_stamp=ReconstructionStamp(
            lineage_task_id=op._lineage_task_id_for(2),
            reconstruction_plan_ids=frozenset({"plan_a"}),
        ),
    )

    assert op._context_task_idx(fresh) == 7
    assert op._context_task_idx(None) == 7
    assert op._context_task_idx(rerun_of_task_2) == 2
    assert op._next_data_task_idx == 7


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
