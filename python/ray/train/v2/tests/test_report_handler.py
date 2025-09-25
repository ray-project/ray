import random
import unittest.mock
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
        validation_spec=None,
    )
    dummy_tr = _TrainingReport(
        metrics={},
        checkpoint=None,
        validation_spec=None,
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
def test_report_handler(tmp_path, num_workers, num_ckpt, num_dummy, num_none, expected):
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

    checkpoint_handler.before_worker_group_shutdown(worker_group)
    worker_group.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
