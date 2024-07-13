import logging

from aiohttp.web import Request, Response

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.actor.actor_head import actor_table_data_to_dict
from ray.dashboard.modules.job.common import JobInfoStorageClient
from ray.dashboard.modules.job.utils import find_jobs_by_job_ids
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.DashboardHeadRouteTable


class TrainHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._train_stats_actor = None
        self._job_info_client = None
        self._gcs_actor_info_stub = None

    @routes.get("/api/train/v2/runs")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @DeveloperAPI
    async def get_train_runs(self, req: Request) -> Response:
        try:
            from ray.train._internal.state.schema import (
                TrainRunInfoWithDetails,
                TrainRunsResponse,
            )
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
                await self._add_actor_status_and_update_run_status(train_runs)
                # Sort train runs in reverse chronological order
                train_runs = sorted(
                    train_runs.values(),
                    key=lambda run: run.start_time_ms,
                    reverse=True,
                )
                job_details = await find_jobs_by_job_ids(
                    self._dashboard_head.gcs_aio_client,
                    self._job_info_client,
                    [run.job_id for run in train_runs],
                )
                train_runs_with_details = [
                    TrainRunInfoWithDetails(
                        **run.dict(), job_details=job_details.get(run.job_id)
                    )
                    for run in train_runs
                ]
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
        from ray.train._internal.state.schema import ActorStatusEnum, RunStatusEnum

        actor_status_table = {}
        try:
            logger.info("Getting all actor info from GCS.")
            request = gcs_service_pb2.GetAllActorInfoRequest()
            reply = await self._gcs_actor_info_stub.GetAllActorInfo(request, timeout=5)
            if reply.status.code == 0:
                for message in reply.actor_table_data:
                    actor_table_data = actor_table_data_to_dict(message)
                    actor_status_table[actor_table_data["actorId"]] = actor_table_data[
                        "state"
                    ]
        except Exception:
            logger.exception("Error Getting all actor info from GCS.")

        for train_run in train_runs.values():
            for worker_info in train_run.workers:
                worker_info.status = actor_status_table.get(worker_info.actor_id, None)

            # The train run can be unexpectedly terminated before the final run
            # status was updated. This could be due to errors outside of the training
            # function (e.g., system failure or user interruption) that crashed the
            # train controller.
            # We need to detect this case and mark the train run as ABORTED.
            controller_actor_status = actor_status_table.get(
                train_run.controller_actor_id, None
            )
            if (
                controller_actor_status == ActorStatusEnum.DEAD
                and train_run.run_status == RunStatusEnum.STARTED
            ):
                train_run.run_status = RunStatusEnum.ABORTED
                train_run.status_detail = (
                    "Unexpectedly terminated due to system errors."
                )

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self, server):
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
            )

        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
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
