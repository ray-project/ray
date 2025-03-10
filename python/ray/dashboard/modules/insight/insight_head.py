import logging
import aiohttp.web
import ray
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


class InsightHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)

    @routes.get("/call_graph")
    async def get_call_graph(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return the call graph data by reading from the InsightMonitor HTTP endpoint."""
        try:
            job_id = req.query.get("job_id", "default_job")

            # Get insight monitor address from KV store using gcs_aio_client
            address = await self.gcs_aio_client.internal_kv_get(
                "insight_monitor_address",
                namespace="flowinsight",
                timeout=5,
            )

            if not address:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="InsightMonitor address not found in KV store",
                )

            host, port = address.decode().split(":")

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://{host}:{port}/get_call_graph_data",
                    params={"job_id": job_id},
                ) as response:
                    if response.status != 200:
                        return dashboard_optional_utils.rest_response(
                            success=False,
                            message=f"Error from insight monitor: {await response.text()}",
                        )

                    graph_data = await response.json()

                    return dashboard_optional_utils.rest_response(
                        success=True,
                        message="Call graph data retrieved successfully.",
                        graph_data=graph_data,
                        job_id=job_id,
                    )

        except Exception as e:
            logger.error(f"Error retrieving call graph data: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Error retrieving call graph data: {str(e)}"
            )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
