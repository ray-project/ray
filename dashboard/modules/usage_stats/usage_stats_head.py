import asyncio
import os
import logging
import time

from concurrent.futures import ThreadPoolExecutor

import ray

import ray.dashboard.utils as dashboard_utils
import ray._private.usage.usage_lib as ray_usage_lib
import ray._private.usage.usage_constants as usage_constants

from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)

# Defines whether or not usage data is reported.
USAGE_REPORT_ENABLED = int(os.getenv("RAY_USAGE_STATS_ENABLE", "0")) == 1


class UsageStatsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.cluster_metadata = None
        self.session_dir = dashboard_head.session_dir

    @async_loop_forever(usage_constants.USAGE_REPORT_INTERVAL)
    async def _report_usage(self):
        assert USAGE_REPORT_ENABLED
        if not self.cluster_metadata:
            self.cluster_metadata = ray_usage_lib.get_cluster_metadata(
                ray.experimental.internal_kv.internal_kv_get_gcs_client(),
                num_retries=20,
            )

        data = self.cluster_metadata.copy()
        data["collect_timestamp_ms"] = int(time.time() * 1000)
        ray_usage_lib.validate_schema(data)

        # In order to not block the event loop, we run blocking IOs
        # within a thread pool.
        with ThreadPoolExecutor(max_workers=1) as executor:
            await ray_usage_lib.write_usage_data_async(data, self.session_dir, executor)
            await ray_usage_lib.report_usage_data_async(data, executor)

    async def run(self, server):
        if not USAGE_REPORT_ENABLED:
            logger.info(
                "Usage module won't be started because the usage report is disabled."
            )
            return
        else:
            logger.info("Start the usage stats module.")
            await asyncio.gather(self._report_usage())

    @staticmethod
    def is_minimal_module():
        return True
