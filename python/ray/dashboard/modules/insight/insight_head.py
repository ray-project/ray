import asyncio
import time
import logging
import aiohttp.web
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.modules.node.datacenter import DataSource
from ray._private.test_utils import get_resource_usage
from ray.dashboard.utils import async_loop_forever
import ray._private.ray_constants as ray_constants
from ray.dashboard.modules.insight.insight_prompt import PROMPT_TEMPLATE
from flow_insight import (
    BatchNodePhysicalStatsEvent,
    BatchNodePhysicalStats,
    NodePhysicalStats,
    NodeResourceUsage,
    DeviceInfo,
    BatchServicePhysicalStatsEvent,
    ServicePhysicalStats,
    ServiceState,
    MemoryInfo,
    DeviceType,
    Service,
    ServicePhysicalStatsRecord,
    NodeMemoryInfo,
    MetaInfoRegisterEvent,
)
from ray.util.insight import (
    create_http_insight_client,
)

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


class InsightHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)
        self._insight_client = None
        self._insight_server_address = None
        asyncio.create_task(self._emit_node_physical_stats())

    async def is_insight_server_alive(self):
        if self._insight_server_address is None:
            return False
        insight_client = create_http_insight_client(self._insight_server_address)

        resp = await insight_client.async_ping()
        if not resp["result"]:
            return False
        return True

    def _to_service_state(self, state: str) -> ServiceState:
        if state == "ALIVE":
            return ServiceState.RUNNING
        elif (
            state == "DEPENDENCIES_UNREADY"
            or state == "PENDING_CREATION"
            or state == "RESTARTING"
        ):
            return ServiceState.WAITING
        elif state == "DEAD":
            return ServiceState.TERMINATED
        else:
            return ServiceState.UNKNOWN

    @async_loop_forever(10)
    async def _emit_node_physical_stats(self):
        insight_server_address = await self.gcs_aio_client.internal_kv_get(
            "insight_monitor_address",
            namespace="flowinsight",
            timeout=5,
        )
        if insight_server_address is None:
            return
        self._insight_server_address = insight_server_address.decode()

        if self._insight_client is None:
            self._insight_client = create_http_insight_client(
                self._insight_server_address
            )

            await self._insight_client.async_emit_event(
                MetaInfoRegisterEvent(
                    prompt=PROMPT_TEMPLATE,
                    timestamp=int(time.time() * 1000),
                )
            )

        stats = BatchNodePhysicalStats(
            stats=[],
        )
        resources_data = get_resource_usage(self.gcs_address)
        for node_data in resources_data.batch:
            node_id = node_data.node_id.hex()
            node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
            mem_total, mem_available, _, mem_used = node_physical_stats.get("mem", {})
            node_stats = NodePhysicalStats(
                node_id=node_id,
                resources={},
                devices={},
                cpu_percent=node_physical_stats.get("cpu", 0),
                memory_info=NodeMemoryInfo(
                    total=mem_total, available=mem_available, used=mem_used
                ),
            )
            for resource_name, total in node_data.resources_total.items():
                available = node_data.resources_available.get(resource_name, 0.0)
                node_stats.resources[resource_name] = NodeResourceUsage(
                    total=total, available=available
                )
            node_physical_stats = DataSource.node_physical_stats.get(node_id, {})
            if "gpus" in node_physical_stats:
                gpus = []
                for gpu in node_physical_stats["gpus"]:
                    gpus.append(
                        DeviceInfo(
                            index=gpu["index"],
                            name=gpu["name"],
                            uuid=gpu["uuid"],
                            memory_total=gpu["memoryTotal"],
                            memory_used=gpu["memoryUsed"],
                            utilization=gpu["utilizationGpu"],
                        )
                    )
                node_stats.devices[DeviceType.GPU] = gpus
            stats.stats.append(node_stats)

        await self._insight_client.async_emit_event(
            BatchNodePhysicalStatsEvent(stats=stats, timestamp=int(time.time() * 1000))
        )

        all_service_stats = {}
        # Add actor information
        for actor_id, actor_info in DataSource.actors.items():
            flow_id = actor_info.get("jobId")
            if actor_info.get("name", "") != "":
                actor_name = actor_info.get("name")
            else:
                actor_name = actor_info.get("className", "Unknown")

            if actor_name == "_ray_internal_insight_monitor":
                continue

            node_id = actor_info["address"]["rayletId"]
            if node_id in DataSource.node_physical_stats:
                actor_pid = actor_info.get("pid")
                service_stats = ServicePhysicalStats(
                    node_id=node_id,
                    pid=actor_pid,
                    state=self._to_service_state(actor_info.get("state", "UNKNOWN")),
                    required_resources=actor_info.get("requiredResources", {}),
                    placement_id=actor_info.get("placementGroupId", None),
                    cpu_percent=0,
                    memory_info=MemoryInfo(
                        rss=0, vms=0, shared=0, text=0, lib=0, data=0, dirty=0
                    ),
                    devices={
                        DeviceType.GPU: [],
                    },
                )
                if actor_pid and "workers" in DataSource.node_physical_stats[node_id]:
                    for worker in node_physical_stats["workers"]:
                        if worker.get("pid") == actor_pid:
                            service_stats.cpu_percent = worker.get("cpuPercent", 0)
                            memory_info = worker.get(
                                "memoryInfo",
                                {
                                    "rss": 0,
                                    "vms": 0,
                                    "shared": 0,
                                    "text": 0,
                                    "lib": 0,
                                    "data": 0,
                                    "dirty": 0,
                                },
                            )
                            service_stats.memory_info = MemoryInfo(
                                rss=memory_info["rss"],
                                vms=memory_info["vms"],
                                shared=memory_info["shared"],
                                text=memory_info["text"],
                                lib=memory_info["lib"],
                                data=memory_info["data"],
                                dirty=memory_info["dirty"],
                            )
                            break

                if actor_pid:
                    node_gpus = DataSource.node_physical_stats[node_id].get("gpus", [])
                    for gpu in node_gpus:
                        for process in gpu.get("processesPids", []):
                            if process["pid"] == actor_pid:
                                service_stats.devices[DeviceType.GPU].append(
                                    DeviceInfo(
                                        index=gpu["index"],
                                        name=gpu["name"],
                                        uuid=gpu["uuid"],
                                        memory_used=process["gpuMemoryUsage"],
                                        memory_total=gpu["memoryTotal"],
                                        utilization=gpu.get("utilizationGpu", 0),
                                    )
                                )
                if flow_id not in all_service_stats:
                    all_service_stats[flow_id] = []
                all_service_stats[flow_id].append(
                    ServicePhysicalStatsRecord(
                        service=Service(service_name=actor_name, instance_id=actor_id),
                        stats=service_stats,
                    )
                )

        for flow_id, service_stats in all_service_stats.items():
            await self._insight_client.async_emit_event(
                BatchServicePhysicalStatsEvent(
                    flow_id=flow_id,
                    stats=service_stats,
                    timestamp=int(time.time() * 1000),
                )
            )

    @routes.get("/insight/{path:.*}")
    async def proxy_get_request(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        """Proxy GET requests to the insight monitor service."""
        return await self._proxy_request(req, "GET")

    @routes.post("/insight/{path:.*}")
    async def proxy_post_request(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """Proxy POST requests to the insight monitor service."""
        return await self._proxy_request(req, "POST")

    async def _proxy_request(
        self, req: aiohttp.web.Request, method: str
    ) -> aiohttp.web.Response:
        """Helper method to proxy requests to the insight monitor service."""
        try:
            path = req.match_info["path"]
            query_string = req.query_string

            if self._insight_server_address is None:
                insight_server_address = await self.gcs_aio_client.internal_kv_get(
                    "insight_monitor_address",
                    namespace="flowinsight",
                    timeout=5,
                )
                if insight_server_address is None:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="InsightMonitor address not found in KV store",
                    )

                self._insight_server_address = insight_server_address.decode()

            if not await self.is_insight_server_alive():
                self._insight_server_address = (
                    await self.gcs_aio_client.internal_kv_get(
                        "insight_monitor_address",
                        namespace="flowinsight",
                        timeout=5,
                    )
                )
                if self._insight_server_address is None:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message="InsightMonitor address not found in KV store",
                    )
                self._insight_server_address = self._insight_server_address.decode()
                self._insight_client = None

            # Get insight monitor address
            target_url = f"http://{self._insight_server_address}/{path}"
            if query_string:
                target_url += f"?{query_string}"

            # Forward the request
            async with aiohttp.ClientSession() as session:
                if method == "GET":
                    async with session.get(
                        target_url, headers=dict(req.headers)
                    ) as resp:
                        body = await resp.read()
                        return aiohttp.web.Response(
                            body=body,
                            status=resp.status,
                            headers=resp.headers,
                        )
                elif method == "POST":
                    data = await req.read()
                    async with session.post(
                        target_url, data=data, headers=dict(req.headers)
                    ) as resp:
                        body = await resp.read()
                        return aiohttp.web.Response(
                            body=body,
                            status=resp.status,
                            headers=resp.headers,
                        )
                else:
                    return dashboard_optional_utils.rest_response(
                        success=False,
                        message=f"Unsupported method: {method}",
                    )
        except Exception as e:
            logger.error(f"Error proxying request to insight monitor: {str(e)}")
            return dashboard_optional_utils.rest_response(
                success=False,
                message=f"Error proxying request to insight monitor: {str(e)}",
            )

    async def run(self):
        pass

    @staticmethod
    def is_minimal_module():
        return False
