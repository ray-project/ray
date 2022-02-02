import asyncio
import os
import logging
import json
import requests
import time

import ray

import ray.dashboard.utils as dashboard_utils
import ray._private.usage_report as ray_usage_lib
import ray.ray_constants as ray_constants

from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)

# TODO(sang): Make below values dynamically configurable.
# Defines whether or not usage data is reported.
USAGE_REPORT_ENABLED = int(os.getenv("RAY_USAGE_STATS_ENABLE", "0")) == 1
# Defines how often usage data is reported to <link>.
USAGE_REPOT_DATA_REPORT_INTERVAL = int(
    os.getenv("RAY_USAGE_STATS_COLLECTION_INTERVAL", 10)
)
USAGE_REPORT_SERVER_URL = "https://dashboard-ses-jAXR6GB7BmsJuRZSawg3a4Ue.anyscale-internal-hsrczdm-0000.anyscale-test-production.com/serve/"  # noqa


class UsageStatsHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self.cluster_metadata = None

    @async_loop_forever(USAGE_REPOT_DATA_REPORT_INTERVAL)
    async def _report_usage(self):
        assert USAGE_REPORT_ENABLED
        if not self.cluster_metadata:
            cluster_metadata = ray.experimental.internal_kv._internal_kv_get(
                ray_usage_lib.CLUSTER_METADATA_KEY,
                namespace=ray_constants.KV_NAMESPACE_CLUSTER,
            )
            self.cluster_metadata = json.loads(cluster_metadata.decode())
            logger.info(f"Updated cluster metadata: {self.cluster_metadata}")
        try:
            self.cluster_metadata["_collect_timestamp_ms"] = time.time()
            logger.info("Send, ", self.cluster_metadata)
            r = requests.request(
                "POST",
                USAGE_REPORT_SERVER_URL,
                headers={"Content-Type": "application/json"},
                cookies={"anyscale-token": "8325e2bf-cbb6-4b4b-995f-98bfd1419796"},
                json=self.cluster_metadata,
            )
            r.raise_for_status()

            logger.info(f"Status code: {r.status_code}, body: {r.json()}")
        except Exception as e:
            logger.exception(e)

    async def run(self, server):
        if not USAGE_REPORT_ENABLED:
            logger.info(
                "Usage module won't be started because " "the usage repot is disabled."
            )
        else:
            await asyncio.gather(self._report_usage())

    @staticmethod
    def is_optional():
        return False
