import logging

from packaging.version import Version

import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.optional_deps import aiohttp, aiohttp_cors, hdrs
from ray._common.utils import get_or_create_event_loop

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardAgentRouteTable


class HttpServerAgent:
    def __init__(self, ip, listen_port):
        self.ip = ip
        self.listen_port = listen_port
        self.http_host = None
        self.http_port = None
        self.http_session = None
        self.runner = None

    async def start(self, modules):
        # What if we.. didn't do that
        pass

    async def cleanup(self):
        # Turns out not a ton to do here
        pass
