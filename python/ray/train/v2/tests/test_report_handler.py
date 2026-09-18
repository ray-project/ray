import random
import unittest.mock
from collections import deque
from unittest.mock import MagicMock

import pytest

from ray.air.config import CheckpointConfig
from ray.train import Checkpoint
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.checkpoint.report_handler import (
    ReportCallbackHandler,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerStatus,
)
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2.tests.util import DummyObjectRefWrapper, DummyWorkerGroup


def generate_worker_group_poll_status(num_workers, num_ckpt, num_dummy, num_none):
    """Generate a WorkerGroupPollStatus object with num_workers workers,
    num_ckpt workers with checkpoint, num_dummy workers with dummy training result,
    and num_none workers with None training result.
    """

    assert num_workers == num_ckpt + num_dummy + num_none
    ckpt_tr = _TrainingReport(
        metrics={},
        checkpoint=Checkpoint("mock://bucket/path"),
        validation=False,
    )
    dummy_tr = _TrainingReport(
        metrics={},
        checkpoint=None,
        validation=False,
    )
    ckpt_ws = WorkerStatus(running=True, error=None, training_report=ckpt_tr)
    dummy_ws = WorkerStatus(running=True, error=None, training_report=dummy_tr)
    none_ws = WorkerStatus(running=True, error=None, training_report=None)

    worker_statuses = (
        [ckpt_ws] * num_ckpt + [dummy_ws] * num_dummy + [none_ws] * num_none
    )
    random.shuffle(worker_statuses)

    return WorkerGroupPollStatus(dict(enumerate(worker_statuses)))


@pytest.mark.parametrize(
    "num_workers, num_ckpt, num_dummy, num_none, expected",
    [
        (10, 1, 9, 0, 1),  # one worker with checkpoint
        (10, 0, 10, 0, 0),  # everyone report metrics only
        (10, 1, 8, 1, 0),  # one worker with checkpoint, one worker with None
    ],
)
@pytest.mark.asyncio
async def test_report_handler(
    tmp_path, num_workers, num_ckpt, num_dummy, num_none, expected
):
    """`expected` is the number of times that the
    CheckpointManager.register_checkpoint is called.
    """
    checkpoint_manager = CheckpointManager(
        storage_context=StorageContext(
            storage_path=tmp_path, experiment_dir_name="test_checkpoint_handler_dir"
        ),
        checkpoint_config=CheckpointConfig(),
    )
    checkpoint_handler = ReportCallbackHandler(report_callbacks=[checkpoint_manager])

    worker_group_context = WorkerGroupContext(
        run_attempt_id="test_run_attempt_id",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=10,
        resources_per_worker={"CPU": 1},
    )
    worker_group = DummyWorkerGroup(
        train_run_context=MagicMock(spec=TrainRunContext),
        worker_group_context=worker_group_context,
    )
    worker_group._start()
    checkpoint_handler.after_worker_group_start(worker_group)

    worker_group_status = generate_worker_group_poll_status(
        num_workers, num_ckpt, num_dummy, num_none
    )
    with unittest.mock.patch.object(
        CheckpointManager, "register_checkpoint", autospec=True
    ) as fake_register_checkpoint:
        checkpoint_handler.after_worker_group_poll_status(worker_group_status)
        assert fake_register_checkpoint.call_count == expected

    checkpoint_handler.after_replica_group_start(worker_group.get_replica_groups()[0])
    assert checkpoint_handler._training_report_queues == [
        deque() for _ in range(num_workers)
    ]

    checkpoint_handler.before_worker_group_shutdown(worker_group)
    worker_group.shutdown()


def _make_handler(num_workers, report_callback):
    """A ReportCallbackHandler wired to a started dummy worker group."""
    handler = ReportCallbackHandler(report_callbacks=[report_callback])
    worker_group = DummyWorkerGroup(
        train_run_context=MagicMock(spec=TrainRunContext),
        worker_group_context=WorkerGroupContext(
            run_attempt_id="test_run_attempt_id",
            train_fn_ref=DummyObjectRefWrapper(lambda: None),
            num_workers=num_workers,
            resources_per_worker={"CPU": 1},
        ),
    )
    worker_group._start()
    handler.after_worker_group_start(worker_group)
    return handler, worker_group


def _poll_status(reporting_ranks, num_workers):
    """A poll where only `reporting_ranks` reported a checkpoint."""
    statuses = {}
    for rank in range(num_workers):
        report = (
            _TrainingReport(
                metrics={"rank": rank},
                checkpoint=Checkpoint("mock://bucket/path"),
                validation=False,
            )
            if rank in reporting_ranks
            else None
        )
        statuses[rank] = WorkerStatus(running=True, error=None, training_report=report)
    return WorkerGroupPollStatus(statuses)


@pytest.mark.asyncio
async def test_relaxed_gate_consolidates_without_preempted_rank():
    """With the gate relaxed, survivors' reports commit without rank 3."""
    num_workers = 4
    callback = MagicMock()
    handler, worker_group = _make_handler(num_workers, callback)

    # Strict gate: rank 3 never reports, so nothing is consolidated.
    handler.after_worker_group_poll_status(_poll_status({0, 1, 2}, num_workers))
    callback.after_report.assert_not_called()

    # Relax to the survivors and replay: now it commits.
    handler.set_expected_ranks([0, 1, 2])
    handler.after_worker_group_poll_status(_poll_status({0, 1, 2}, num_workers))

    callback.after_report.assert_called_once()
    kwargs = callback.after_report.call_args.kwargs
    assert kwargs["training_report"].checkpoint is not None
    # One entry per *expected* rank, and rank 0's metrics lead.
    assert kwargs["metrics"] == [{"rank": 0}, {"rank": 1}, {"rank": 2}]

    handler.before_worker_group_shutdown(worker_group)
    worker_group.shutdown()


@pytest.mark.asyncio
async def test_relaxed_gate_drops_skipped_ranks_stale_reports():
    """A skipped rank's queued report must not be paired with a later round."""
    num_workers = 3
    callback = MagicMock()
    handler, worker_group = _make_handler(num_workers, callback)

    # Rank 2 reported before it was preempted; the round is still incomplete.
    handler.after_worker_group_poll_status(_poll_status({2}, num_workers))
    assert len(handler._training_report_queues[2]) == 1

    handler.set_expected_ranks([0, 1])
    handler.after_worker_group_poll_status(_poll_status({0, 1}, num_workers))

    callback.after_report.assert_called_once()
    # Rank 2's stale report was consumed with its round, not carried forward.
    assert not handler._training_report_queues[2]

    handler.before_worker_group_shutdown(worker_group)
    worker_group.shutdown()


@pytest.mark.asyncio
async def test_expected_ranks_reset_on_worker_group_restart():
    """A relaxed gate belongs to the preemption, not to the next worker group."""
    num_workers = 3
    callback = MagicMock()
    handler, worker_group = _make_handler(num_workers, callback)

    handler.set_expected_ranks([0, 1])
    assert handler._get_expected_indices() == [0, 1]

    handler.before_worker_group_shutdown(worker_group)
    assert handler._expected_ranks is None

    handler.after_worker_group_start(worker_group)
    assert handler._get_expected_indices() == [0, 1, 2]

    # And the strict gate really is back: rank 2 must report again.
    handler.after_worker_group_poll_status(_poll_status({0, 1}, num_workers))
    callback.after_report.assert_not_called()

    handler.before_worker_group_shutdown(worker_group)
    worker_group.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
