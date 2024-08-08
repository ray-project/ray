import json
import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from aiohttp.web import Request, Response

import ray
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class TrainHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @optional_utils.DashboardHeadRouteTable.get("/api/train/runs/")
    @optional_utils.init_ray_and_catch_exceptions()
    async def get_train_runs(self, req: Request) -> Response:
        try:
            from ray.train._internal.stats import _get_or_create_stats_actor

            _stats_actor = _get_or_create_stats_actor()
            train_runs = ray.get(_stats_actor.get_train_runs.remote())
            train_runs_dicts = [train_run.dict() for train_run in train_runs]

            return Response(
                text=json.dumps({"train_runs": train_runs_dicts}),
                content_type="application/json",
            )
        except Exception as e:
            logging.exception("Exception occurred while getting train runs.")
            return Response(
                status=503,
                text=str(e),
            )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
