import { VirtualCluster } from "../type/virtual_cluster";
import { get } from "./requestHandlers";

export type VirtualClusterRsp = {
  result: boolean;
  msg: string;
  data: {
    result: {
      result: VirtualCluster[];
    };
  };
};

export const getVirtualClusters = async (
  detail?: boolean,
): Promise<VirtualCluster[]> => {
  const url = `/api/v0/vclusters?detail=true`;
  const response = await get<VirtualClusterRsp>(url);
  return response.data.data.result.result.map((vc: any) => ({
    name: vc.virtual_cluster_id,
    divisible: vc.divisible,
    dividedClusters: vc.divided_clusters || {},
    replicaSets: vc.replica_sets || {},
    undividedReplicaSets: vc.undivided_replica_sets || {},
    resourcesUsage: vc.resources_usage || {},
    resources: {
      CPU: vc.resources_usage?.CPU || "0",
      memory: vc.resources_usage?.memory || "0",
      object_store_memory: vc.resources_usage?.object_store_memory || "0",
    },
    visibleNodeInstances: vc.visible_node_instances
      ? Object.fromEntries(
          Object.entries(vc.visible_node_instances).map(
            ([id, instance]: [string, any]) => [
              id,
              {
                hostname: instance.hostname,
                template_id: instance.template_id || "Unknown",
                is_dead: instance.is_dead,
                resources_usage: instance.resources_usage || {},
              },
            ],
          ),
        )
      : undefined,
    undividedNodes: vc.undivided_nodes
      ? Object.fromEntries(
          Object.entries(vc.undivided_nodes).map(
            ([id, instance]: [string, any]) => [
              id,
              {
                hostname: instance.hostname,
                template_id: instance.template_id || "Unknown",
                is_dead: instance.is_dead,
                resources_usage: instance.resources_usage || {},
              },
            ],
          ),
        )
      : undefined,
  }));
};
