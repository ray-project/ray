import asyncio
import logging

import ray

import ray.dashboard.utils as dashboard_utils
import ray._private.usage.usage_lib as ray_usage_lib

from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)


class UsageStatsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.cluster_metadata = None
        self.session_dir = dashboard_head.session_dir
        self.client = ray_usage_lib.UsageReportClient()

    @async_loop_forever(ray_usage_lib._usage_stats_report_interval_s())
    async def _report_usage(self):
        try:
            assert ray_usage_lib._usage_stats_enabled()
            if not self.cluster_metadata:
                self.cluster_metadata = ray_usage_lib.get_cluster_metadata(
                    ray.experimental.internal_kv.internal_kv_get_gcs_client(),
                    num_retries=20,
                )

            data = ray_usage_lib.generate_report_data(self.cluster_metadata)

            # In order to not block the event loop, we run blocking IOs
            # within a thread pool.
            await self.client.write_usage_data_async(data, self.session_dir)
            await self.client.report_usage_data_async(
                ray_usage_lib._usage_stats_report_url(), data
            )
        except:
            # Log nothing if something goes wrong.
            pass

    async def run(self, server):
        if not ray_usage_lib._usage_stats_enabled():
            logger.info("Usage reporting is disabled.")
            return
        else:
            logger.info("Usage reporting is disabled.")
            await asyncio.gather(self._report_usage())

    @staticmethod
    def is_minimal_module():
        return True
