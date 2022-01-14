import os
import asyncio
import logging
from typing import Union

import ray.experimental.internal_kv as internal_kv
import ray.ray_constants as ray_constants
import ray._private.utils as utils
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.consts as dashboard_consts
from ray.dashboard.utils import async_loop_forever, create_task
from ray.dashboard.modules.event import event_consts
from ray.dashboard.modules.event.event_utils import monitor_events
from ray.core.generated import event_pb2
from ray.core.generated import event_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class EventAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._event_dir = os.path.join(self._dashboard_agent.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        self._stub: Union[event_pb2_grpc.ReportEventServiceStub, None] = None
        self._cached_events = asyncio.Queue(
            event_consts.EVENT_AGENT_CACHE_SIZE)
        logger.info("Event agent cache buffer size: %s",
                    self._cached_events.maxsize)

    async def _connect_to_dashboard(self):
        """ Connect to the dashboard. If the dashboard is not started, then
        this method will never returns.

        Returns:
            The ReportEventServiceStub object.
        """
        while True:
            try:
                # TODO: Use async version if performance is an issue
                dashboard_rpc_address = internal_kv._internal_kv_get(
                    dashboard_consts.DASHBOARD_RPC_ADDRESS,
                    namespace=ray_constants.KV_NAMESPACE_DASHBOARD)
                if dashboard_rpc_address:
                    logger.info("Report events to %s", dashboard_rpc_address)
                    options = (("grpc.enable_http_proxy", 0), )
                    channel = utils.init_grpc_channel(
                        dashboard_rpc_address,
                        options=options,
                        asynchronous=True)
                    return event_pb2_grpc.ReportEventServiceStub(channel)
            except Exception:
                logger.exception("Connect to dashboard failed.")
            await asyncio.sleep(
                event_consts.RETRY_CONNECT_TO_DASHBOARD_INTERVAL_SECONDS)

    @async_loop_forever(event_consts.EVENT_AGENT_REPORT_INTERVAL_SECONDS)
    async def report_events(self):
        """ Report events from cached events queue. Reconnect to dashboard if
        report failed. Log error after retry EVENT_AGENT_RETRY_TIMES.

        This method will never returns.
        """
        data = await self._cached_events.get()
        for _ in range(event_consts.EVENT_AGENT_RETRY_TIMES):
            try:
                logger.info("Report %s events.", len(data))
                request = event_pb2.ReportEventsRequest(event_strings=data)
                await self._stub.ReportEvents(request)
                break
            except Exception:
                logger.exception("Report event failed, reconnect to the "
                                 "dashboard.")
                self._stub = await self._connect_to_dashboard()
        else:
            data_str = str(data)
            limit = event_consts.LOG_ERROR_EVENT_STRING_LENGTH_LIMIT
            logger.error("Report event failed: %s",
                         data_str[:limit] + (data_str[limit:] and "..."))

    async def run(self, server):
        # Connect to dashboard.
        self._stub = await self._connect_to_dashboard()
        # Start monitor task.
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: create_task(self._cached_events.put(data)),
            source_types=event_consts.EVENT_AGENT_MONITOR_SOURCE_TYPES)
        # Start reporting events.
        await self.report_events()
