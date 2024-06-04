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
    async def get_train_workers(self, req: Request) -> Response:
        from ray.dashboard.modules.train.schema import TrainRunsResponse

        stats_actor = await self.get_train_stats_actor()

        if stats_actor is None:
            details = TrainRunsResponse(train_runs=[])
        else:
            try:
                train_runs = await stats_actor.get_all_train_runs.remote()
                # TODO(aguo): Sort by created_at
                details = TrainRunsResponse(train_runs=list(train_runs.values()))
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
        """Gets the ServeController to the this cluster's Serve app.

        return: If Serve is running on this Ray cluster, returns a client to
            the Serve controller. If Serve is not running, returns None.
        """
        try:
            from ray.train._internal.state.state_actor import get_state_actor

            if self._train_stats_actor is None:
                self._train_stats_actor = get_state_actor()

            return self._train_stats_actor
        except ModuleNotFoundError:
            logger.exception(
                "Train is not installed. Please run `pip install ray[train]`."
            )
