import mimetypes
import logging
import aiohttp.web
import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.actor_utils as actor_utils
from ray.new_dashboard.utils import rest_response
from ray.new_dashboard.datacenter import DataSource, GlobalSignals, DataOrganizer

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class LogHead(dashboard_utils.DashboardHeadModule):
    LOG_URL_TEMPLATE = "http://{ip}:{port}/logs"

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/logical/actor_groups")
    async def get_actor_groups(self, req) -> aiohttp.web.Response:
        actors = await DataOrganizer.get_all_actors()
        actor_creation_tasks = await DataOrganizer.get_actor_creation_tasks()
        # actor_creation_tasks have some common interface with actors,
        # and they get processed and shown in tandem in the logical view
        # hence we merge them together before constructing actor groups.
        actors.update(actor_creation_tasks)
        actor_groups = actor_utils.construct_actor_groups(actors)
        return await rest_response(success=True, message="Fetched actor groups.", actor_groups=actor_groups)

    async def run(self, server):
        pass