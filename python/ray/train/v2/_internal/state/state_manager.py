import logging
import time
from collections import defaultdict
from typing import Dict, List

from ray.actor import ActorHandle
from ray.train.v2._internal.execution.context import DistributedContext
from ray.train.v2._internal.execution.worker_group import ActorMetadata, Worker
from ray.train.v2._internal.state.schema import (
    ActorStatus,
    RunAttemptStatus,
    RunStatus,
    TrainResources,
    TrainRun,
    TrainRunAttempt,
    TrainWorker,
)
from ray.train.v2._internal.state.state_actor import get_or_create_state_actor

logger = logging.getLogger(__name__)


def _current_time_ms() -> int:
    return int(time.time() * 1000)


class TrainStateManager:
    """Manages the state of a train run and run attempts."""

    def __init__(self) -> None:
        self._state_actor = get_or_create_state_actor()
        # NOTE: All runs and attempts are stored in memory.
        # This may be a memory issue for large runs.
        self._runs: Dict[str, TrainRun] = {}
        # {run_id: {attempt_id: TrainRunAttempt}}
        self._run_attempts: Dict[str, Dict[str, TrainRunAttempt]] = defaultdict(dict)

    def create_train_run(
        self,
        id: str,
        name: str,
        job_id: str,
        controller_actor_id: str,
    ) -> None:
        run = TrainRun(
            id=id,
            name=name,
            job_id=job_id,
            status=RunStatus.INITIALIZING,
            status_detail=None,
            controller_actor_id=controller_actor_id,
            start_time_ms=_current_time_ms(),
        )
        self._runs[run.id] = run
        self._create_or_update_train_run(run)

    def update_train_run_scheduling(
        self,
        run_id: str,
    ) -> None:
        run = self._runs[run_id]
        run.status = RunStatus.SCHEDULING
        run.status_detail = None
        self._create_or_update_train_run(run)

    def update_train_run_running(
        self,
        run_id: str,
    ) -> None:
        run = self._runs[run_id]
        run.status = RunStatus.RUNNING
        run.status_detail = None
        self._create_or_update_train_run(run)

    def update_train_run_restarting(
        self,
        run_id: str,
    ) -> None:
        run = self._runs[run_id]
        run.status = RunStatus.RESTARTING
        run.status_detail = None
        self._create_or_update_train_run(run)

    def update_train_run_resizing(
        self,
        run_id: str,
    ) -> None:
        run = self._runs[run_id]
        run.status = RunStatus.RESIZING
        run.status_detail = None
        self._create_or_update_train_run(run)

    def update_train_run_finished(
        self,
        run_id: str,
    ):
        run = self._runs[run_id]
        run.status = RunStatus.FINISHED
        run.status_detail = None
        run.end_time_ms = _current_time_ms()
        self._create_or_update_train_run(run)

    def update_train_run_errored(
        self,
        run_id: str,
        status_detail: str,
    ):
        run = self._runs[run_id]
        run.status = RunStatus.ERRORED
        run.status_detail = status_detail
        run.end_time_ms = _current_time_ms()
        self._create_or_update_train_run(run)

    # TODO: This may be handled in the StateManager.
    def update_train_run_aborted(
        self,
        run_id: str,
    ):
        run = self._runs[run_id]
        run.status = RunStatus.ABORTED
        run.status_detail = None  # TODO: Add status detail.
        run.end_time_ms = _current_time_ms()
        self._create_or_update_train_run(run)

    def create_train_run_attempt(
        self,
        run_id: str,
        attempt_id: str,
        num_workers: int,
        resources_per_worker: Dict[str, float],
    ) -> None:

        resources = [
            TrainResources(resources=resources_per_worker) for _ in range(num_workers)
        ]

        run_attempt = TrainRunAttempt(
            run_id=run_id,
            attempt_id=attempt_id,
            start_time_ms=_current_time_ms(),
            status=RunAttemptStatus.PENDING,
            status_detail=None,
            resources=resources,
            workers=[],  # Not started yet.
        )

        self._run_attempts[run_id][attempt_id] = run_attempt
        self._create_or_update_train_run_attempt(run_attempt)

    def update_train_run_attempt_running(
        self, run_id: str, attempt_id: str, workers: List[Worker]
    ) -> None:
        def _convert_worker(worker: Worker) -> TrainWorker:

            actor: ActorHandle = worker.actor
            distributed_context: DistributedContext = worker.distributed_context
            actor_metadata: ActorMetadata = worker.metadata

            return TrainWorker(
                world_rank=distributed_context.world_rank,
                local_rank=distributed_context.local_rank,
                node_rank=distributed_context.node_rank,
                actor_id=actor._actor_id.hex(),
                node_id=actor_metadata.node_id,
                node_ip=actor_metadata.node_ip,
                pid=actor_metadata.pid,
                gpu_ids=actor_metadata.gpu_ids,
                status=ActorStatus.ALIVE,
                resources=TrainResources(resources=worker.resources),
            )

        workers: List[TrainWorker] = [_convert_worker(worker) for worker in workers]

        run_attempt = self._run_attempts[run_id][attempt_id]
        run_attempt.status = RunAttemptStatus.RUNNING
        run_attempt.status_detail = None
        run_attempt.workers = workers
        self._create_or_update_train_run_attempt(run_attempt)

    def update_train_run_attempt_finished(
        self,
        run_id: str,
        attempt_id: str,
    ):
        run_attempt = self._run_attempts[run_id][attempt_id]
        run_attempt.status = RunAttemptStatus.FINISHED
        run_attempt.status_detail = None
        run_attempt.end_time_ms = _current_time_ms()
        self._create_or_update_train_run_attempt(run_attempt)

    def update_train_run_attempt_errored(
        self,
        run_id: str,
        attempt_id: str,
        status_detail: str,
    ):
        run_attempt = self._run_attempts[run_id][attempt_id]
        run_attempt.status = RunAttemptStatus.ERRORED
        run_attempt.status_detail = status_detail
        run_attempt.end_time_ms = _current_time_ms()
        self._create_or_update_train_run_attempt(run_attempt)

    def update_train_run_attempt_aborted(
        self,
        run_id: str,
        attempt_id: str,
    ):
        run_attempt = self._run_attempts[run_id][attempt_id]
        run_attempt.status_detail = None  # TODO: Add status detail.
        run_attempt.status = RunAttemptStatus.ABORTED
        run_attempt.end_time_ms = _current_time_ms()
        self._create_or_update_train_run_attempt(run_attempt)

    def _create_or_update_train_run(self, run: TrainRun) -> None:
        self._state_actor.create_or_update_train_run.remote(run)

    def _create_or_update_train_run_attempt(self, run_attempt: TrainRunAttempt) -> None:
        self._state_actor.create_or_update_train_run_attempt.remote(run_attempt)
