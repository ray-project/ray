import aiohttp
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from typing import Union, Optional, List

import ray.dashboard.utils as dashboard_utils
from ray.core.generated import event_pb2
from ray.dashboard.modules.event import event_consts
from ray.dashboard.modules.event.event_utils import monitor_events
from ray.dashboard.utils import async_loop_forever, create_task

logger = logging.getLogger(__name__)


class EventAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._event_dir = os.path.join(self._dashboard_agent.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        self._cached_events = asyncio.Queue(event_consts.EVENT_AGENT_CACHE_SIZE)
        self._gcs_aio_client = dashboard_agent.gcs_aio_client
        self.monitor_thread_pool_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="event_monitor"
        )
        self._dashboard_address: Optional[str] = None
        # Total number of event created from this agent.
        self.total_event_reported = 0
        # Total number of event report request sent.
        self.total_request_sent = 0
        self.module_started = time.monotonic()

        logger.info("Event agent cache buffer size: %s", self._cached_events.maxsize)

    async def report_events_once(self, data: List[str]):
        """
        Report events to the dashboard once.

        Raises exception if the request failed.

        TODO(ryw): understand if this serialization needs to be in a thread pool.
        """
        assert self._dashboard_address is not None, "Dashboard address is not set."

        request = event_pb2.ReportEventsRequest(event_strings=data)
        serialized_proto = request.SerializeToString()
        with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://{self._dashboard_address}/report_events",
                data=serialized_proto,
            ) as response:
                response.raise_for_status()
                self.total_request_sent += 1

    @async_loop_forever(event_consts.EVENT_AGENT_REPORT_INTERVAL_SECONDS)
    async def report_events(self):
        """Report events from cached events queue. Reconnect to dashboard if
        report failed. Log error after retry EVENT_AGENT_RETRY_TIMES.

        A call to this coroutine never completes.
        """
        if self._dashboard_address is None:
            self._dashboard_address = await dashboard_utils.try_get_api_server_url(
                self._gcs_aio_client
            )
            if self._dashboard_address is None:
                logger.info(
                    "Cannot get dashboard address. Maybe it's not started. Wait"
                    " for the next loop."
                )
                return

        data = await self._cached_events.get()
        self.total_event_reported += len(data)
        for _ in range(event_consts.EVENT_AGENT_RETRY_TIMES):
            try:
                logger.debug("Reporting %s events.", len(data))
                await self.report_events_once(data)
            except Exception:
                wait_s = event_consts.RETRY_CONNECT_TO_DASHBOARD_INTERVAL_SECONDS
                logger.exception(f"Report event failed, waiting {wait_s}s and retry...")
                await asyncio.sleep(wait_s)
        else:
            data_str = str(data)
            limit = event_consts.LOG_ERROR_EVENT_STRING_LENGTH_LIMIT
            logger.error(
                "Report event failed: %s",
                data_str[:limit] + (data_str[limit:] and "..."),
            )

    async def get_internal_states(self):
        if self.total_event_reported <= 0 or self.total_request_sent <= 0:
            return

        elapsed = time.monotonic() - self.module_started
        return {
            "total_events_reported": self.total_event_reported,
            "Total_report_request": self.total_request_sent,
            "queue_size": self._cached_events.qsize(),
            "total_uptime": elapsed,
        }

    async def run(self, server):
        # Start monitor task.
        self._monitor = monitor_events(
            self._event_dir,
            lambda data: create_task(self._cached_events.put(data)),
            self.monitor_thread_pool_executor,
        )

        await asyncio.gather(
            self.report_events(),
        )

    @staticmethod
    def is_minimal_module():
        return False
