import logging

from aiohttp.web import Request, Response
import ray
from ray.util.annotations import DeveloperAPI

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.job.common import (
    JobInfoStorageClient,
)
from ray.dashboard.modules.job.utils import (
    find_jobs_by_job_ids,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.DashboardHeadRouteTable


class TrainHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._train_stats_actor = None
        self._job_info_client = None

    # TODO(aguo): Update this to a "v2" path since I made a backwards-incompatible
    # change. Will do so after the API is more stable.
    @routes.get("/api/train/runs")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @DeveloperAPI
    async def get_train_runs(self, req: Request) -> Response:
        try:
            from ray.train._internal.state.schema import (
                TrainRunsResponse,
                TrainRunInfoWithDetails,
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

    @staticmethod
    def is_minimal_module():
        return False

    async def run(self, server):
        if not self._job_info_client:
            self._job_info_client = JobInfoStorageClient(
                self._dashboard_head.gcs_aio_client
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
