import os
import time
import uuid
from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock

from ray.train import Checkpoint
from ray.train._internal.session import _TrainingResult
from ray.train.context import TrainContext
from ray.train.v2._internal.execution.context import (
    DistributedContext,
    TrainRunContext,
)
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupContext,
    WorkerGroupPollStatus,
    WorkerGroupState,
    WorkerStatus,
)
from ray.train.v2._internal.state.schema import (
    ActorStatus,
    RunAttemptStatus,
    RunStatus,
    TrainResources,
    TrainRun,
    TrainRunAttempt,
    TrainWorker,
)
from ray.train.v2._internal.util import ObjectRefWrapper, time_monotonic
from ray.train.v2.api.exceptions import TrainingFailedError


class DummyWorkerGroup(WorkerGroup):

    _start_failure = None

    # TODO: Clean this up and use Mocks instead.
    def __init__(
        self,
        train_run_context: TrainRunContext,
        worker_group_context: WorkerGroupContext,
        callbacks=None,
    ):
        self._num_workers = worker_group_context.num_workers
        self._worker_group_state = None
        self._worker_statuses = {}

    def poll_status(self, *args, **kwargs) -> WorkerGroupPollStatus:
        return WorkerGroupPollStatus(
            worker_statuses=self._worker_statuses,
        )

    def _start(self):
        num_workers = self._num_workers
        if self._start_failure:
            raise self._start_failure

        self._worker_group_state = WorkerGroupState(
            start_time=time_monotonic(),
            workers=[MagicMock() for i in range(num_workers)],
            placement_group=MagicMock(),
            sync_actor=None,
        )

        self._worker_statuses = {
            i: WorkerStatus(running=True, error=None) for i in range(num_workers)
        }

    def shutdown(self):
        self._worker_group_state = None

    def abort(self):
        pass

    # === Test methods ===
    def error_worker(self, worker_index):
        status = self._worker_statuses[worker_index]
        status.error = RuntimeError(f"Worker {worker_index} failed")

    def finish_worker(self, worker_index):
        status = self._worker_statuses[worker_index]
        status.running = False

    @classmethod
    def set_start_failure(cls, start_failure):
        cls._start_failure = start_failure


class MockScalingPolicy(ScalingPolicy):
    def __init__(self, scaling_config):
        self._recovery_decision_queue = []
        self._monitor_decision_queue = []

        super().__init__(scaling_config)

    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        if self._recovery_decision_queue:
            return self._recovery_decision_queue.pop(0)
        return NoopDecision()

    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        if self._monitor_decision_queue:
            return self._monitor_decision_queue.pop(0)
        return NoopDecision()

    # === Test methods ===
    def queue_recovery_decision(self, decision):
        self._recovery_decision_queue.append(decision)

    def queue_monitor_decision(self, decision):
        self._monitor_decision_queue.append(decision)


class MockFailurePolicy(FailurePolicy):
    def __init__(self, failure_config):
        self._decision_queue = []

        super().__init__(failure_config)

    def make_decision(
        self, training_failed_error: TrainingFailedError
    ) -> FailureDecision:
        if self._decision_queue:
            return self._decision_queue.pop(0)
        return FailureDecision.NOOP

    # === Test methods ===
    def queue_decision(self, decision):
        self._decision_queue.append(decision)


class DummyObjectRefWrapper(ObjectRefWrapper):
    """Mock object that returns the object passed in without going through ray.put."""

    def __init__(self, obj):
        self._obj = obj

    def get(self):
        return self._obj


_RUN_ID = "mock_run_id"


def create_mock_train_run(
    status: RunStatus = RunStatus.RUNNING,
    controller_actor_id: Optional[str] = None,
    end_time_ns: Optional[int] = None,
    id: Optional[str] = None,
    status_detail: Optional[str] = None,
):
    return TrainRun(
        schema_version=0,
        id=id or _RUN_ID,
        name="test_run",
        job_id=uuid.uuid4().hex,
        controller_actor_id=controller_actor_id or uuid.uuid4().hex,
        status=status,
        status_detail=status_detail,
        start_time_ns=time.time_ns(),
        end_time_ns=end_time_ns,
        controller_log_file_path="/tmp/ray/session_xxx/logs/train/ray-train-app-controller.log",
    )


def create_mock_train_run_attempt(
    attempt_id: str = "mock_attempt_id",
    status: RunAttemptStatus = RunAttemptStatus.RUNNING,
    end_time_ns: Optional[int] = None,
    run_id: Optional[str] = None,
    worker_status: Optional[ActorStatus] = ActorStatus.ALIVE,
    status_detail: Optional[str] = None,
):
    worker = TrainWorker(
        world_rank=0,
        local_rank=0,
        node_rank=0,
        actor_id=uuid.uuid4().hex,
        node_id=uuid.uuid4().hex,
        node_ip="127.0.0.1",
        pid=1234,
        gpu_ids=[0],
        status=worker_status,
        resources=TrainResources(resources={"CPU": 1}),
        log_file_path="/tmp/ray/session_xxx/logs/train/ray-train-app-worker.log",
    )

    return TrainRunAttempt(
        schema_version=0,
        attempt_id=attempt_id,
        run_id=run_id or _RUN_ID,
        status=status,
        status_detail=status_detail,
        start_time_ns=time.time_ns(),
        resources=[TrainResources(resources={"CPU": 1})],
        workers=[worker],
        end_time_ns=end_time_ns,
    )


def create_dummy_run_context(**kwargs: dict) -> TrainRunContext:
    """Create a standardized TrainRunContext for testing.

    Args:
        **kwargs: Optional overrides for the default configuration.

    Returns:
        TrainRunContext: A standardized TrainRunContext instance for testing.
    """
    from ray.train import BackendConfig, DataConfig
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2.api.config import RunConfig, ScalingConfig

    config = dict(
        run_config=RunConfig(name="test"),
        train_loop_config={},
        scaling_config=ScalingConfig(num_workers=1),
        backend_config=BackendConfig(),
        datasets={},
        dataset_config=DataConfig(),
    )
    config.update(kwargs)
    return TrainRunContext(**config)


class DummyTrainContext(TrainContext):
    """A dummy TrainContext subclass for testing."""

    def __init__(self):
        self.train_run_context = create_dummy_run_context()
        self.distributed_context = DistributedContext(
            world_rank=0,
            world_size=1,
            local_rank=0,
            local_world_size=1,
            node_rank=0,
        )
        # Mock everything else since we don't need the actual functionality
        self.execution_context = MagicMock()
        self.storage_context = MagicMock()
        self.dataset_shards = {}

    def get_run_config(self):
        return self.train_run_context.run_config


def create_dummy_train_context() -> TrainContext:
    """Create a standardized TrainContext for testing.

    Returns:
        TrainContext: A standardized TrainContext instance for testing.
    """
    return DummyTrainContext()


def create_dummy_training_results(
    num_results: int,
    storage_context: StorageContext,
    include_metrics: bool = True,
) -> List[_TrainingResult]:
    training_results = []
    for i in range(num_results):
        metrics = {"score": i} if include_metrics else {}
        checkpoint_path = os.path.join(
            storage_context.experiment_fs_path, f"checkpoint_{i}"
        )
        os.makedirs(checkpoint_path, exist_ok=True)
        training_results.append(
            _TrainingResult(
                checkpoint=Checkpoint(
                    path=Path(checkpoint_path).as_posix(),
                    filesystem=storage_context.storage_filesystem,
                ),
                metrics=metrics,
            )
        )
    return training_results
