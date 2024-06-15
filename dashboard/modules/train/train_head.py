import logging

from aiohttp.web import Request, Response
import ray
from ray.util.annotations import DeveloperAPI

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = dashboard_optional_utils.DashboardHeadRouteTable


class TrainHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._train_stats_actor = None

    @routes.get("/api/train/runs")
    @dashboard_optional_utils.init_ray_and_catch_exceptions()
    @DeveloperAPI
    async def get_train_runs(self, req: Request) -> Response:
        try:
            from ray.train._internal.state.schema import (
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
            details = TrainRunsResponse(train_runs=[])
        else:
            try:
                train_runs = await stats_actor.get_all_train_runs.remote()
                # Sort train runs in reverse chronological order
                train_runs = sorted(
                    train_runs.values(),
                    key=lambda run: run.start_time,
                    reverse=True,
                )
                details = TrainRunsResponse(train_runs=train_runs)
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
        pass

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
