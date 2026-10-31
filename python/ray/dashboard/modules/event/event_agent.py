import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Union

import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
from ray._private.authentication.http_token_authentication import (
    get_auth_headers_if_auth_enabled,
)
from ray._private.gcs_passive_utils import PassiveLatch
from ray.dashboard.modules.event import event_consts
from ray.dashboard.modules.event.event_utils import monitor_events
from ray.dashboard.utils import async_loop_forever, create_task

logger = logging.getLogger(__name__)


# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_EVENT_AGENT_TPE_MAX_WORKERS = ray_constants.env_integer(
    "RAY_DASHBOARD_EVENT_AGENT_TPE_MAX_WORKERS", 1
)


class EventAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._event_dir = os.path.join(self._dashboard_agent.log_dir, "events")
        os.makedirs(self._event_dir, exist_ok=True)
        self._monitor: Union[asyncio.Task, None] = None
        # Lazy initialized on first use. Once initialized, it will not be
        # changed.
        self._dashboard_http_address = None
        self._cached_events = asyncio.Queue(event_consts.EVENT_AGENT_CACHE_SIZE)
        self._gcs_client = dashboard_agent.gcs_client
        # Total number of event created from this agent.
        self.total_event_reported = 0
        # Total number of event report request sent.
        self.total_request_sent = 0
        self.module_started = time.monotonic()

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_EVENT_AGENT_TPE_MAX_WORKERS,
            thread_name_prefix="event_agent_executor",
        )

        logger.info("Event agent cache buffer size: %s", self._cached_events.maxsize)
        self._address_passive_latch = PassiveLatch(
            "dashboard http address",
            logger,
            action_desc_passive=(
                "GCS is in passive mode or dashboard address not published yet. "
                "Waiting for dashboard address."
            ),
            action_desc_promoted="Dashboard http address resolved: %s",
        )

    async def _get_dashboard_http_address(self):
        """
        Lazily get the dashboard http address from InternalKV. If it's not set, sleep
        and retry forever.
        """
        while True:
            if self._dashboard_http_address:
                return self._dashboard_http_address
            try:
                dashboard_http_address = await self._gcs_client.async_internal_kv_get(
                    ray_constants.DASHBOARD_ADDRESS.encode(),
                    namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
                    timeout=dashboard_consts.GCS_RPC_TIMEOUT_SECONDS,
                )
                if not dashboard_http_address:
                    raise ValueError("Dashboard http address not found in InternalKV.")
                address = dashboard_http_address.decode()
                if not address.startswith(("http://", "https://")):
                    address = f"http://{address}"
                self._dashboard_http_address = address
                self._address_passive_latch.promoted(address)
                return self._dashboard_http_address
            except Exception as e:
                if ray_constants.RAY_ENABLE_GCS_LEADER_ELECTION and isinstance(
                    e, ValueError
                ):
                    self._address_passive_latch.note_passive()
                else:
                    logger.exception("Get dashboard http address failed.")
            await asyncio.sleep(1)

    @async_loop_forever(event_consts.EVENT_AGENT_REPORT_INTERVAL_SECONDS)
    async def report_events(self):
        """Report events from cached events queue. Reconnect to dashboard if
        report failed. Log error after retry EVENT_AGENT_RETRY_TIMES.

        This method will never returns.
        """
        dashboard_http_address = await self._get_dashboard_http_address()
        data = await self._cached_events.get()
        self.total_event_reported += len(data)
        last_exception = None
        for _ in range(event_consts.EVENT_AGENT_RETRY_TIMES):
            try:
                logger.debug("Report %s events.", len(data))
                async with self._dashboard_agent.http_session.post(
                    f"{dashboard_http_address}/report_events",
                    json=data,
                    headers=get_auth_headers_if_auth_enabled({}),
                ) as response:
                    response.raise_for_status()
                self.total_request_sent += 1
                break
            except Exception as e:
                logger.warning(f"Report event failed, retrying... {e}")
                last_exception = e
        else:
            data_str = str(data)
            limit = event_consts.LOG_ERROR_EVENT_STRING_LENGTH_LIMIT
            logger.error(
                "Report event failed: %s",
                data_str[:limit] + (data_str[limit:] and "..."),
                exc_info=last_exception,
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
            self._executor,
        )

        await asyncio.gather(
            self.report_events(),
        )

    @staticmethod
    def is_minimal_module():
        return False
