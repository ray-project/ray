import os
import asyncio
import logging
import random
from concurrent.futures import ThreadPoolExecutor

import ray

import ray.dashboard.utils as dashboard_utils
import ray._private.usage.usage_lib as ray_usage_lib

from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)


class UsageStatsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.usage_stats_enabled = ray_usage_lib.usage_stats_enabled()
        self.usage_stats_prompt_enabled = ray_usage_lib.usage_stats_prompt_enabled()
        self.cluster_metadata = ray_usage_lib.get_cluster_metadata(
            ray.experimental.internal_kv.internal_kv_get_gcs_client(),
            num_retries=20,
        )
        self.cluster_config_to_report = ray_usage_lib.get_cluster_config_to_report(
            os.path.expanduser("~/ray_bootstrap_config.yaml")
        )
        self.session_dir = dashboard_head.session_dir
        self.client = ray_usage_lib.UsageReportClient()
        # The total number of report succeeded.
        self.total_success = 0
        # The total number of report failed.
        self.total_failed = 0
        # The seq number of report. It increments whenever a new report is sent.
        self.seq_no = 0

    if ray._private.utils.check_dashboard_dependencies_installed():
        import aiohttp

        routes = ray.dashboard.optional_utils.ClassMethodRouteTable

        @routes.get("/usage_stats_enabled")
        async def get_usage_stats_enabled(self, req) -> aiohttp.web.Response:
            return ray.dashboard.optional_utils.rest_response(
                success=True,
                message="Fetched usage stats enabled",
                usage_stats_enabled=self.usage_stats_enabled,
                usage_stats_prompt_enabled=self.usage_stats_prompt_enabled,
            )

    def _report_usage_sync(self):
        """
        - Always write usage_stats.json regardless of report success/failure.
        - If report fails, the error message should be written to usage_stats.json
        - If file write fails, the error will just stay at dashboard.log.
            usage_stats.json won't be written.
        """
        if not self.usage_stats_enabled:
            return

        try:
            data = ray_usage_lib.generate_report_data(
                self.cluster_metadata,
                self.cluster_config_to_report,
                self.total_success,
                self.total_failed,
                self.seq_no,
            )

            error = None
            try:
                self.client.report_usage_data(
                    ray_usage_lib._usage_stats_report_url(), data
                )
            except Exception as e:
                logger.info(f"Usage report request failed. {e}")
                error = str(e)
                self.total_failed += 1
            else:
                self.total_success += 1
            finally:
                self.seq_no += 1

            data = ray_usage_lib.generate_write_data(data, error)
            self.client.write_usage_data(data, self.session_dir)
        except Exception as e:
            logger.exception(e)
            logger.info(f"Usage report failed: {e}")

    async def _report_usage_async(self):
        if not self.usage_stats_enabled:
            return

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            await loop.run_in_executor(executor, self._report_usage_sync)

    @async_loop_forever(ray_usage_lib._usage_stats_report_interval_s())
    async def periodically_report_usage(self):
        await self._report_usage_async()

    async def run(self, server):
        if not self.usage_stats_enabled:
            logger.info("Usage reporting is disabled.")
            return
        else:
            logger.info("Usage reporting is enabled.")
            # Wait for 1 minutes to send the first report
            # so autoscaler has the chance to set DEBUG_AUTOSCALING_STATUS.
            await asyncio.sleep(min(60, ray_usage_lib._usage_stats_report_interval_s()))
            await self._report_usage_async()
            # Add a random offset before the second report to remove sample bias.
            await asyncio.sleep(
                random.randint(0, ray_usage_lib._usage_stats_report_interval_s())
            )
            await asyncio.gather(self.periodically_report_usage())

    @staticmethod
    def is_minimal_module():
        return True
