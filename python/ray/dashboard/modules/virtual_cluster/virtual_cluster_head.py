import logging

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_service_pb2 import (
    CreateOrUpdateVirtualClusterRequest,
    GetVirtualClustersRequest,
    RemoveVirtualClusterRequest,
)

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


class VirtualClusterHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/virtual_clusters")
    @dashboard_optional_utils.aiohttp_cache(10)
    async def get_all_virtual_clusters(self, req) -> aiohttp.web.Response:
        reply = await self._gcs_virtual_cluster_info_stub.GetVirtualClusters(
            GetVirtualClustersRequest()
        )

        if reply.status.code == 0:
            data = dashboard_utils.message_to_dict(
                reply, always_print_fields_with_no_presence=True
            )
            for virtual_cluster_data in data.get("virtualClusterDataList", []):
                virtual_cluster_data["virtualClusterId"] = virtual_cluster_data.pop(
                    "id"
                )
                virtual_cluster_data["revision"] = int(
                    virtual_cluster_data.get("revision", 0)
                )
                virtual_cluster_data["divisible"] = str(
                    virtual_cluster_data.pop("divisible", False)
                ).lower()

            return dashboard_optional_utils.rest_response(
                success=True,
                message="All virtual clusters fetched.",
                virtual_clusters=data.get("virtualClusterDataList", []),
            )
        else:
            logger.info("Failed to get all virtual clusters")
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Failed to get all virtual clusters: {}".format(
                    reply.status.message
                ),
            )

    @routes.post("/virtual_clusters")
    async def create_or_update_virtual_cluster(self, req) -> aiohttp.web.Response:
        virtual_cluster_info_json = await req.json()
        logger.info("POST /virtual_clusters %s", virtual_cluster_info_json)

        virtual_cluster_info = dict(virtual_cluster_info_json)
        virtual_cluster_id = virtual_cluster_info["virtualClusterId"]
        divisible = False
        if str(virtual_cluster_info.get("divisible", False)).lower() == "true":
            divisible = True

        request = CreateOrUpdateVirtualClusterRequest(
            virtual_cluster_id=virtual_cluster_id,
            divisible=divisible,
            replica_sets=virtual_cluster_info.get("replicaSets", {}),
            revision=int(virtual_cluster_info.get("revision", 0)),
        )
        reply = await (
            self._gcs_virtual_cluster_info_stub.CreateOrUpdateVirtualCluster(request)
        )
        data = dashboard_utils.message_to_dict(
            reply, always_print_fields_with_no_presence=True
        )

        if reply.status.code == 0:
            logger.info("Virtual cluster %s created or updated", virtual_cluster_id)

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Virtual cluster created or updated.",
                virtual_cluster_id=virtual_cluster_id,
                revision=int(data.get("revision", 0)),
                node_instances=data.get("nodeInstances", {}),
            )
        else:
            logger.info(
                "Failed to create or update virtual cluster %s", virtual_cluster_id
            )
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Failed to create or update virtual cluster {}: {}".format(
                    virtual_cluster_id, reply.status.message
                ),
                virtual_cluster_id=virtual_cluster_id,
                replica_sets_to_recommend=data.get("replicaSetsToRecommend", {}),
            )

    @routes.delete("/virtual_clusters/{virtual_cluster_id}")
    async def remove_virtual_cluster(self, req) -> aiohttp.web.Response:
        virtual_cluster_id = req.match_info.get("virtual_cluster_id")
        request = RemoveVirtualClusterRequest(virtual_cluster_id=virtual_cluster_id)
        reply = await self._gcs_virtual_cluster_info_stub.RemoveVirtualCluster(request)

        if reply.status.code == 0:
            logger.info("Virtual cluster %s removed", virtual_cluster_id)
            return dashboard_optional_utils.rest_response(
                success=True,
                message=f"Virtual cluster {virtual_cluster_id} removed.",
                virtual_cluster_id=virtual_cluster_id,
            )
        else:
            logger.info("Failed to remove virtual cluster %s", virtual_cluster_id)
            return dashboard_optional_utils.rest_response(
                success=False,
                message="Failed to remove virtual cluster {}: {}".format(
                    virtual_cluster_id, reply.status.message
                ),
                virtual_cluster_id=virtual_cluster_id,
            )

    async def run(self, server):
        self._gcs_virtual_cluster_info_stub = (
            gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(
                self.aiogrpc_gcs_channel
            )
        )

    @staticmethod
    def is_minimal_module():
        return False
