import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2_grpc
from ray.dashboard.datacenter import DataOrganizer
from ray.dashboard.modules.job.common import JobInfoStorageClient
from ray.dashboard.modules.job.utils import find_jobs_by_job_ids
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.dashboard.modules.job.pydantic_models import JobDetails
    from ray.train.v2._internal.state.schema import (
        DecoratedTrainRun,
        DecoratedTrainRunAttempt,
        DecoratedTrainWorker,
        RunStatus,
        TrainRun,
        TrainRunAttempt,
        TrainWorker,
    )

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.DashboardHeadRouteTable


class TrainHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._train_stats_actor = None  # Train V1
        self._train_v2_state_actor = None  # Train V2
        self._job_info_client = None
        self._gcs_actor_info_stub = None

    # TODO: The next iteration of this should be "/api/train/v2/runs/v2".
    # This follows the naming convention of "/api/train/{train_version}/runs/{api_version}".
    # This API corresponds to the Train V2 API.
    @routes.get("/api/train/v2/runs/v1")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @DeveloperAPI
    async def get_train_v2_runs(self, req: Request) -> Response:
        """Get all TrainRuns for Ray Train V2."""

        try:
            from ray.train.v2._internal.state.schema import TrainRunsResponse
        except ImportError:
            logger.exception(
                "Train is not installed. Please run `pip install ray[train]` "
                "when setting up Ray on your cluster."
            )
            return Response(
                status=500,
                text="Train is not installed. Please run `pip install ray[train]` "
                "when setting up Ray on your cluster.",
            )

        state_actor = await self.get_train_v2_state_actor()

        if state_actor is None:
            return Response(
                status=500,
                text=(
                    "Train state data is not available. Please make sure Ray Train "
                    "is running and that the Train state actor is enabled by setting "
                    'the RAY_TRAIN_ENABLE_STATE_TRACKING environment variable to "1".'
                ),
            )
        else:
            try:
                train_runs = await state_actor.get_train_runs.remote()
                decorated_train_runs = await self._decorate_train_runs(
                    train_runs.values()
                )
                details = TrainRunsResponse(train_runs=decorated_train_runs)
            except ray.exceptions.RayTaskError as e:
                # Task failure sometimes are due to GCS
                # failure. When GCS failed, we expect a longer time
                # to recover.
                return Response(
                    status=503,
                    text=(
                        "Failed to get a response from the train stats actor. "
                        f"The GCS may be down, please retry later: {e}"
                    ),
                )

        return Response(
            text=details.json(),
            content_type="application/json",
        )

    async def _decorate_train_runs(
        self, train_runs: List["TrainRun"]
    ) -> List["DecoratedTrainRun"]:
        """Decorate the train runs with run attempts, job details, status, and status details.

        Returns:
            List[DecoratedTrainRun]: The decorated train runs in reverse chronological order.
        """

        from ray.train.v2._internal.state.schema import DecoratedTrainRun

        decorated_train_runs: List[DecoratedTrainRun] = []

        state_actor = await self.get_train_v2_state_actor()
        all_train_run_attempts = await state_actor.get_train_run_attempts.remote()

        jobs = await self._get_jobs([train_run.job_id for train_run in train_runs])

        for train_run in train_runs:

            # TODO: Batch these together across TrainRuns if needed.
            train_run_attempts = all_train_run_attempts[train_run.id].values()
            decorated_train_run_attempts: List[
                DecoratedTrainRunAttempt
            ] = await self._decorate_train_run_attempts(train_run_attempts)

            job_details = jobs[train_run.job_id]

            status, status_details = await self._get_run_status(train_run)

            decorated_train_run = DecoratedTrainRun.parse_obj(
                {
                    **train_run.dict(),
                    "attempts": decorated_train_run_attempts,
                    "job_details": job_details,
                    "status": status,
                    "status_detail": status_details,
                }
            )

            decorated_train_runs.append(decorated_train_run)

        # Sort train runs in reverse chronological order
        decorated_train_runs = sorted(
            decorated_train_runs,
            key=lambda run: run.start_time_ms,
            reverse=True,
        )
        return decorated_train_runs

    async def _get_jobs(self, job_ids: List[str]) -> Dict[str, "JobDetails"]:
        return await find_jobs_by_job_ids(
            self.gcs_aio_client,
            self._job_info_client,
            job_ids,
        )

    async def _decorate_train_run_attempts(
        self, train_run_attempts: List["TrainRunAttempt"]
    ) -> List["DecoratedTrainRunAttempt"]:
        from ray.train.v2._internal.state.schema import DecoratedTrainRunAttempt

        decorated_train_run_attempts: List[DecoratedTrainRunAttempt] = []

        for train_run_attempt in train_run_attempts:

            # TODO: Batch these together across TrainRunAttempts if needed.
            decorated_train_workers: List[
                DecoratedTrainWorker
            ] = await self._decorate_train_workers(train_run_attempt.workers)

            decorated_train_run_attempt = DecoratedTrainRunAttempt.parse_obj(
                {**train_run_attempt.dict(), "workers": decorated_train_workers}
            )
            decorated_train_run_attempts.append(decorated_train_run_attempt)

        return decorated_train_run_attempts

    async def _decorate_train_workers(
        self, train_workers: List["TrainWorker"]
    ) -> List["DecoratedTrainWorker"]:
        from ray.train.v2._internal.state.schema import DecoratedTrainWorker

        decorated_train_workers: List[DecoratedTrainWorker] = []

        actor_ids = [worker.actor_id for worker in train_workers]

        logger.info(f"Getting all actor info from GCS (actor_ids={actor_ids})")

        train_run_actors = await DataOrganizer.get_actor_infos(
            actor_ids=actor_ids,
        )

        for train_worker in train_workers:
            actor = train_run_actors.get(train_worker.actor_id, None)
            # Add hardware metrics to API response
            if actor:
                gpus = [
                    gpu
                    for gpu in actor["gpus"]
                    if train_worker.pid
                    in [process["pid"] for process in gpu["processesPids"]]
                ]
                # Need to convert processesPids into a proper list.
                # It's some weird ImmutableList structure
                # We also convert the list of processes into a single item since
                # an actor is only a single process and cannot match multiple
                # processes.
                formatted_gpus = [
                    {
                        **gpu,
                        "processInfo": [
                            process
                            for process in gpu["processesPids"]
                            if process["pid"] == train_worker.pid
                        ][0],
                    }
                    for gpu in gpus
                ]

                decorated_train_worker = DecoratedTrainWorker.parse_obj(
                    {
                        **train_worker.dict(),
                        "status": actor["state"],
                        "processStats": actor["processStats"],
                        "gpus": formatted_gpus,
                    }
                )
            else:
                decorated_train_worker = DecoratedTrainWorker.parse_obj(
                    train_worker.dict()
                )

            decorated_train_workers.append(decorated_train_worker)
        return decorated_train_workers

    async def _get_run_status(
        self, train_run: "TrainRun"
    ) -> Tuple["RunStatus", Optional[str]]:
        from ray.train.v2._internal.state.schema import ActorStatus, RunStatus

        # TODO: Move this to the TrainStateActor.

        # The train run can be unexpectedly terminated before the final run
        # status was updated. This could be due to errors outside of the training
        # function (e.g., system failure or user interruption) that crashed the
        # train controller.
        # We need to detect this case and mark the train run as ABORTED.

        actor_infos = await DataOrganizer.get_actor_infos(
            actor_ids=[train_run.controller_actor_id],
        )
        controller_actor_info = actor_infos[train_run.controller_actor_id]

        controller_actor_status = (
            controller_actor_info.get("state") if controller_actor_info else None
        )
        if (
            controller_actor_status == ActorStatus.DEAD
            and train_run.status == RunStatus.RUNNING
        ):
            run_status = RunStatus.ABORTED
            status_detail = "Terminated due to system errors or killed by the user."
            return (run_status, status_detail)

        # Default to original.
        return (train_run.status, train_run.status_detail)

    # TODO: The next iteration of this should be "/api/train/v1/runs/v3".
    # This follows the naming convention of "/api/train/{train_version}/runs/{api_version}".
    # This API corresponds to the Train V1 API.
    @routes.get("/api/train/v2/runs")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @DeveloperAPI
    async def get_train_runs(self, req: Request) -> Response:
        """Get all TrainRunInfos for Ray Train V1."""
        try:
            from ray.train._internal.state.schema import TrainRunsResponse
        except ImportError:
            logger.exception(
                "Train is not installed. Please run `pip install ray[train]` "
                "when setting up Ray on your cluster."
            )
            return Response(
                status=500,
                text="Train is not installed. Please run `pip install ray[train]` "
                "when setting up Ray on your cluster.",
            )

        stats_actor = await self.get_train_stats_actor()

        if stats_actor is None:
            return Response(
                status=500,
                text=(
                    "Train state data is not available. Please make sure Ray Train "
                    "is running and that the Train state actor is enabled by setting "
                    'the RAY_TRAIN_ENABLE_STATE_TRACKING environment variable to "1".'
                ),
            )
        else:
            try:
                train_runs = await stats_actor.get_all_train_runs.remote()
                train_runs_with_details = (
                    await self._add_actor_status_and_update_run_status(train_runs)
                )
                # Sort train runs in reverse chronological order
                train_runs_with_details = sorted(
                    train_runs_with_details,
                    key=lambda run: run.start_time_ms,
                    reverse=True,
                )
                job_details = await find_jobs_by_job_ids(
                    self.gcs_aio_client,
                    self._job_info_client,
                    [run.job_id for run in train_runs_with_details],
                )
                for run in train_runs_with_details:
                    run.job_details = job_details.get(run.job_id)
                details = TrainRunsResponse(train_runs=train_runs_with_details)
            except ray.exceptions.RayTaskError as e:
                # Task failure sometimes are due to GCS
                # failure. When GCS failed, we expect a longer time
                # to recover.
                return Response(
                    status=503,
                    text=(
                        "Failed to get a response from the train stats actor. "
                        f"The GCS may be down, please retry later: {e}"
                    ),
                )

        return Response(
            text=details.json(),
            content_type="application/json",
        )

    async def _add_actor_status_and_update_run_status(self, train_runs):
        from ray.train._internal.state.schema import (
            ActorStatusEnum,
            RunStatusEnum,
            TrainRunInfoWithDetails,
            TrainWorkerInfoWithDetails,
        )

        train_runs_with_details: List[TrainRunInfoWithDetails] = []

        for train_run in train_runs.values():
            worker_infos_with_details: List[TrainWorkerInfoWithDetails] = []

            actor_ids = [worker.actor_id for worker in train_run.workers]

            logger.info(f"Getting all actor info from GCS (actor_ids={actor_ids})")

            train_run_actors = await DataOrganizer.get_actor_infos(
                actor_ids=actor_ids,
            )

            for worker_info in train_run.workers:
                actor = train_run_actors.get(worker_info.actor_id, None)
                # Add hardware metrics to API response
                if actor:
                    gpus = [
                        gpu
                        for gpu in actor["gpus"]
                        if worker_info.pid
                        in [process["pid"] for process in gpu["processesPids"]]
                    ]
                    # Need to convert processesPids into a proper list.
                    # It's some weird ImmutableList structureo
                    # We also convert the list of processes into a single item since
                    # an actor is only a single process and cannot match multiple
                    # processes.
                    formatted_gpus = [
                        {
                            **gpu,
                            "processInfo": [
                                process
                                for process in gpu["processesPids"]
                                if process["pid"] == worker_info.pid
                            ][0],
                        }
                        for gpu in gpus
                    ]

                    worker_info_with_details = TrainWorkerInfoWithDetails.parse_obj(
                        {
                            **worker_info.dict(),
                            "status": actor["state"],
                            "processStats": actor["processStats"],
                            "gpus": formatted_gpus,
                        }
                    )
                else:
                    worker_info_with_details = TrainWorkerInfoWithDetails.parse_obj(
                        worker_info.dict()
                    )

                worker_infos_with_details.append(worker_info_with_details)

            train_run_with_details = TrainRunInfoWithDetails.parse_obj(
                {**train_run.dict(), "workers": worker_infos_with_details}
            )

            # The train run can be unexpectedly terminated before the final run
            # status was updated. This could be due to errors outside of the training
            # function (e.g., system failure or user interruption) that crashed the
            # train controller.
            # We need to detect this case and mark the train run as ABORTED.
            actor = train_run_actors.get(train_run.controller_actor_id)
            controller_actor_status = actor.get("state") if actor else None
            if (
                controller_actor_status == ActorStatusEnum.DEAD
                and train_run.run_status == RunStatusEnum.RUNNING
            ):
                train_run_with_details.run_status = RunStatusEnum.ABORTED
                train_run_with_details.status_detail = (
                    "Terminated due to system errors or killed by the user."
                )

            train_runs_with_details.append(train_run_with_details)

        return train_runs_with_details

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self, server):
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(self.gcs_aio_client)

        gcs_channel = self.aiogrpc_gcs_channel
        self._gcs_actor_info_stub = gcs_service_pb2_grpc.ActorInfoGcsServiceStub(
            gcs_channel
        )

    async def get_train_stats_actor(self):
        """
        Gets the train stats actor and caches it as an instance variable.
        """
        try:
            from ray.train._internal.state.state_actor import get_state_actor

            if self._train_stats_actor is None:
                self._train_stats_actor = get_state_actor()

            return self._train_stats_actor
        except ImportError:
            logger.exception(
                "Train is not installed. Please run `pip install ray[train]` "
                "when setting up Ray on your cluster."
            )
        return None

    async def get_train_v2_state_actor(self):
        """
        Gets the Train state actor and caches it as an instance variable.
        """
        try:
            from ray.train.v2._internal.state.state_actor import get_state_actor

            if self._train_v2_state_actor is None:
                self._train_v2_state_actor = get_state_actor()

            return self._train_v2_state_actor
        except ImportError:
            logger.exception(
                "Train is not installed. Please run `pip install ray[train]` "
                "when setting up Ray on your cluster."
            )
        return None
