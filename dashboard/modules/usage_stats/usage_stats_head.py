import asyncio
import logging
import random

import ray

import ray.dashboard.utils as dashboard_utils
import ray._private.usage.usage_lib as ray_usage_lib

from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)


class UsageStatsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.cluster_metadata = ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(),
            num_retries=20,
        )
        self.session_dir = dashboard_head.session_dir
        self.client = ray_usage_lib.UsageReportClient()
        # The total number of report succeeded.
        self.report_success = 0
        # The total number of report failed.
        self.report_failed = 0

    @async_loop_forever(ray_usage_lib._usage_stats_report_interval_s())
    async def _report_usage(self):
        if not ray_usage_lib._usage_stats_enabled():
            return

        """
        - Always write usage_stats.json regardless of report success/failure.
        - If report fails, the error message should be written to usage_stats.json
        - If file write fails, the error will just stay at dashboard.log.
            usage_stats.json won't be written.
        """
        try:
            data = ray_usage_lib.generate_report_data(self.cluster_metadata)
            error_message = None
            try:
                await self.client.report_usage_data_async(
                    ray_usage_lib._usage_stats_report_url(), data
                )
            except Exception as e:
                logger.info(
                    "Usage report request failed. "
                    "It won't affect any of Ray operations, except "
                    "the usage stats won't be reported to "
                    f"{ray_usage_lib._usage_stats_report_url()}. "
                    f"Error message: {e}"
                )
                error_message = str(e)
                self.report_failed += 1
            else:
                self.report_success += 1

            data = ray_usage_lib.generate_write_data(
                data, error_message, self.report_success, self.report_failed
            )
            await self.client.write_usage_data_async(data, self.session_dir)

        except Exception as e:
            logger.info(
                "Usage report failed due to unexpected error. "
                f"It won't affect any of Ray operations. Error message: {e}"
            )

        # Add a random offset to remove sample bias.
        await asyncio.sleep(
            random.randint(0, ray_usage_lib._usage_stats_report_interval_s())
        )

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
